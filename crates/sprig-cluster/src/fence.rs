//! Shard fences: blocking writes to one shard inside MariaDB, so a gateway
//! with a stale shard map can't write (chapter 09, "Fencing primitives";
//! build step 7).
//!
//! Two methods:
//!
//! - [`FenceMethod::RevokeKill`], the default: revoke the tenant's write
//!   privileges (plus `SELECT`, `EXECUTE`, `SHOW VIEW` and `SHOW CREATE
//!   ROUTINE` for [`FenceScope::All`]) on the shard schemas, then kill the tenant's
//!   sessions that could still write, and wait until they are gone.
//! - [`FenceMethod::Rename`], which needs no `CONNECTION ADMIN`: one atomic
//!   `RENAME TABLE` moves every table to `__sprig_f_<hex16(name)>`.
//!
//! Every fence returns a [`FenceReceipt`]; [`unfence`] replays it exactly.
//! A fence that fails part-way undoes what it did before returning the
//! error, so the caller never holds half a fence.
//!
//! Only names that parse as the tenant's own schemas and accounts are
//! touched: `sprig_ctl` never revokes from or kills anything else
//! (chapter 09, "Security notes").

use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::hash::BuildHasher;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sprig_common::ids::{NodeId, TenantId};
use sprig_common::names::{AccountName, SchemaName, internal_object, quote, tenant_account_like};
use sprig_common::{Result, SprigCode, SprigError};
use twox_hash::XxHash3_64;

pub use crate::profile::FenceMethod;
use crate::settings::ClusterSettings;
use crate::sql::{NodeEndpoint, ProbeError, ProbeSession, TextRow, literal};

/// A binlog position as MariaDB prints it: a comma-separated GTID list such
/// as `0-1-42,2-201-17`.
///
/// Until `sprig-binlog` exposes its own position type this is the text
/// form; chapter 10 compares positions per domain.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GtidPos(pub String);

impl fmt::Display for GtidPos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What a fence blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FenceScope {
    /// Writes only; reads continue (the cutover's first fence).
    Writes,
    /// Reads too (after the epoch flip, so stale gateways can't read).
    All,
}

/// Why a whole cluster fences itself (managed clusters, R2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FenceReason {
    /// The writer lease was lost (chapter 25).
    LeaseLost,
    /// The data center is fenced (chapter 25).
    DcFenced,
    /// An operator asked.
    Operator,
}

/// A request to fence one shard on its cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceRequest {
    /// The tenant: only its accounts are revoked from and killed.
    pub tenant: TenantId,
    /// The shard's physical schemas; each must belong to `tenant`.
    pub schemas: Vec<String>,
    /// For `Rename`, only these tables (in every schema); `None` for all.
    /// `RevokeKill` always fences whole schemas.
    pub tables: Option<Vec<String>>,
    /// What to block.
    pub scope: FenceScope,
    /// How.
    pub method: FenceMethod,
    /// The fence fails if it can't finish by then.
    pub deadline: Instant,
}

/// One revoked database-level grant, exactly as it was.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevokedGrant {
    /// The account or role name.
    pub user: String,
    /// The host part; empty for a role.
    pub host: String,
    /// `mysql.db.Db` as stored (possibly with `\_` escapes).
    pub db: String,
    /// The privileges revoked, in SQL spelling (`INSERT`, `LOCK TABLES`).
    pub privileges: Vec<String>,
}

impl RevokedGrant {
    /// Whether this grant belongs to a role (roles have an empty host).
    #[must_use]
    pub fn is_role(&self) -> bool {
        self.host.is_empty()
    }

    fn grantee(&self) -> String {
        if self.is_role() {
            literal(&self.user)
        } else {
            format!("{}@{}", literal(&self.user), literal(&self.host))
        }
    }

    /// `REVOKE ... ON db.* FROM grantee`.
    #[must_use]
    pub fn revoke_sql(&self) -> String {
        format!(
            "REVOKE {} ON {}.* FROM {}",
            self.privileges.join(", "),
            quote(&self.db),
            self.grantee()
        )
    }

    /// `GRANT ... ON db.* TO grantee`: the undo of [`revoke_sql`](Self::revoke_sql).
    #[must_use]
    pub fn grant_sql(&self) -> String {
        format!(
            "GRANT {} ON {}.* TO {}",
            self.privileges.join(", "),
            quote(&self.db),
            self.grantee()
        )
    }
}

/// One renamed table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RenamedTable {
    /// The schema.
    pub schema: String,
    /// The tenant's name.
    pub from: String,
    /// The fenced name, `__sprig_f_<hex16>`.
    pub to: String,
}

/// What a fence did, so [`unfence`] can undo exactly that.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FenceReceipt {
    /// The tenant.
    pub tenant: TenantId,
    /// The method used.
    pub method: FenceMethod,
    /// The scope.
    pub scope: FenceScope,
    /// The node the grants or tables were changed on (the shard writer).
    pub writer: NodeId,
    /// Revoked grants (`RevokeKill`).
    pub revoked: Vec<RevokedGrant>,
    /// Renamed tables (`Rename`).
    pub renamed: Vec<RenamedTable>,
    /// Killed sessions: node and connection id.
    pub killed: Vec<(NodeId, u64)>,
    /// `@@gtid_binlog_pos` after the fence (`Rename`: `@@last_gtid`, the
    /// rename's own GTID). No write to the shard follows it in the binlog.
    pub writer_pos: GtidPos,
    /// `Rename`: the writer's `UNIX_TIMESTAMP()` just before the rename. A
    /// table under a renamed table's own name created at or after it is a
    /// stray recreation, which [`unfence`] may drop if it is empty; `None`
    /// in planned receipts and in receipts written before the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fenced_at_unix: Option<u64>,
}

/// Tunables of a fence, from `cluster.*` settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceOptions {
    /// `cluster.kill_wait_ms`: how long killed sessions may take to go.
    pub kill_wait: Duration,
    /// `cluster.fence_lock_wait_seconds`: `lock_wait_timeout` of a rename.
    pub lock_wait_seconds: u64,
    /// `cluster.probe_timeout_ms`: each round trip, except the rename,
    /// which gets the lock wait on top.
    pub timeout: Duration,
    /// `performance_schema` is on: kill only sessions holding metadata locks
    /// on the schemas instead of every one with an open transaction.
    pub use_performance_schema: bool,
}

impl FenceOptions {
    /// From resolved settings.
    #[must_use]
    pub fn from_settings(s: &ClusterSettings, use_performance_schema: bool) -> Self {
        Self {
            kill_wait: s.kill_wait,
            lock_wait_seconds: s.fence_lock_wait_seconds,
            timeout: s.probe_timeout,
            use_performance_schema,
        }
    }
}

impl Default for FenceOptions {
    fn default() -> Self {
        Self::from_settings(&ClusterSettings::default(), false)
    }
}

/// How often killed sessions are polled.
const KILL_POLL: Duration = Duration::from_millis(20);

/// Privileges a `Writes` fence revokes: every database privilege that can
/// change data or schema. `mysql.db` column and SQL spelling.
pub const WRITE_PRIVILEGES: &[(&str, &str)] = &[
    ("Insert_priv", "INSERT"),
    ("Update_priv", "UPDATE"),
    ("Delete_priv", "DELETE"),
    ("Create_priv", "CREATE"),
    ("Drop_priv", "DROP"),
    ("References_priv", "REFERENCES"),
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

/// What an `All` fence revokes on top of [`WRITE_PRIVILEGES`].
pub const READ_PRIVILEGES: &[(&str, &str)] = &[
    ("Select_priv", "SELECT"),
    ("Execute_priv", "EXECUTE"),
    ("Show_view_priv", "SHOW VIEW"),
    // 11.3+; absent on older servers, so never held there.
    ("Show_create_routine_priv", "SHOW CREATE ROUTINE"),
];

/// The fenced name of a table: `__sprig_f_<16 hex digits of xxh3_64(name)>`.
#[must_use]
pub fn fenced_name(table: &str) -> String {
    internal_object(&format!("f_{:016x}", XxHash3_64::oneshot(table.as_bytes())))
}

/// Whether `table` is a fenced name.
#[must_use]
pub fn is_fenced_name(table: &str) -> bool {
    table
        .strip_prefix(&internal_object("f_"))
        .is_some_and(|h| h.len() == 16 && h.bytes().all(|b| b.is_ascii_hexdigit()))
}

fn fail(what: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::ClusterFenceFailed, what.into())
}

fn fail_with(what: &str, e: ProbeError) -> SprigError {
    fail(format!("{what}: {e}")).with_source(e)
}

/// Checks that every schema belongs to `tenant`.
fn check_schemas(tenant: TenantId, schemas: &[String]) -> Result<()> {
    if schemas.is_empty() {
        return Err(SprigError::internal("a fence needs at least one schema"));
    }
    for s in schemas {
        match s.parse::<SchemaName>() {
            Ok(n) if n.tenant() == tenant => {}
            _ => {
                return Err(SprigError::internal(format!(
                    "fence of tenant {tenant}: {s:?} is not one of its schemas"
                )));
            }
        }
    }
    Ok(())
}

fn in_list<'a>(items: impl IntoIterator<Item = &'a str>) -> String {
    items
        .into_iter()
        .map(literal)
        .collect::<Vec<_>>()
        .join(", ")
}

/// `mysql.db` stores a literal `_` either as is or escaped as `\_`.
fn db_spellings(schema: &str) -> [String; 2] {
    [
        schema.to_owned(),
        schema.replace('_', "\\_").replace('%', "\\%"),
    ]
}

fn unescape_db(db: &str) -> String {
    db.replace("\\_", "_").replace("\\%", "%")
}

fn col<'a>(row: &'a TextRow, name: &str) -> Option<&'a str> {
    row.iter()
        .find(|(c, _)| c.eq_ignore_ascii_case(name))
        .and_then(|(_, v)| v.as_deref())
}

fn first_col(row: &TextRow) -> Option<&str> {
    row.first().and_then(|(_, v)| v.as_deref())
}

/// Picks what to revoke from `mysql.db` rows: the tenant's accounts on the
/// fenced schemas, and of each only the privileges it holds.
#[must_use]
pub fn grants_to_revoke(
    rows: &[TextRow],
    tenant: TenantId,
    schemas: &[String],
    scope: FenceScope,
) -> Vec<RevokedGrant> {
    let wanted: Vec<&(&str, &str)> = match scope {
        FenceScope::Writes => WRITE_PRIVILEGES.iter().collect(),
        FenceScope::All => WRITE_PRIVILEGES.iter().chain(READ_PRIVILEGES).collect(),
    };
    let mut out = Vec::new();
    for row in rows {
        let (Some(user), Some(host), Some(db)) =
            (col(row, "User"), col(row, "Host"), col(row, "Db"))
        else {
            continue;
        };
        let ours = user
            .parse::<AccountName>()
            .is_ok_and(|a| a.tenant() == tenant);
        if !ours || !schemas.iter().any(|s| *s == unescape_db(db)) {
            continue;
        }
        let privileges: Vec<String> = wanted
            .iter()
            .filter(|(c, _)| col(row, c).is_some_and(|v| v.eq_ignore_ascii_case("Y")))
            .map(|(_, sql)| (*sql).to_owned())
            .collect();
        if !privileges.is_empty() {
            out.push(RevokedGrant {
                user: user.to_owned(),
                host: host.to_owned(),
                db: db.to_owned(),
                privileges,
            });
        }
    }
    out.sort_by(|a, b| (&a.db, &a.user, &a.host).cmp(&(&b.db, &b.user, &b.host)));
    out
}

/// One row of `information_schema.PROCESSLIST`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRow {
    /// Connection id.
    pub id: u64,
    /// Account name.
    pub user: String,
    /// Current database.
    pub db: Option<String>,
    /// Running statement.
    pub info: Option<String>,
}

/// The sessions a `RevokeKill` fence kills on one node: the tenant's own
/// sessions whose current database is fenced (database privileges reach
/// open connections only at their next `USE`), whose running statement
/// names a fenced schema, or which may hold uncommitted writes to it.
///
/// "May hold writes" is `mdl_holders` (sessions holding metadata locks on
/// the schemas, with `performance_schema`) when known, else every session
/// with an open `InnoDB` transaction.
#[must_use]
pub fn sessions_to_kill<S: BuildHasher>(
    rows: &[SessionRow],
    tenant: TenantId,
    schemas: &[String],
    open_trx: &HashSet<u64, S>,
    mdl_holders: Option<&HashSet<u64, S>>,
) -> Vec<u64> {
    let mut out: Vec<u64> = rows
        .iter()
        .filter(|r| {
            r.user
                .parse::<AccountName>()
                .is_ok_and(|a| a.tenant() == tenant)
        })
        .filter(|r| {
            let in_db = r.db.as_ref().is_some_and(|d| schemas.contains(d));
            let names = r
                .info
                .as_ref()
                .is_some_and(|i| schemas.iter().any(|s| i.contains(s.as_str())));
            let writes = match mdl_holders {
                Some(m) => m.contains(&r.id),
                None => open_trx.contains(&r.id),
            };
            in_db || names || writes
        })
        .map(|r| r.id)
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// Fences one shard: `writer` is the shard's writer, where grants or tables
/// change; `kill_on` are the nodes whose tenant sessions are killed
/// (`RevokeKill` only).
///
/// # Errors
/// 39103 if the fence couldn't finish (sessions outlived
/// `cluster.kill_wait_ms`, the rename waited past its lock wait, the
/// deadline passed, or a statement was refused); everything done so far is
/// undone first. `Internal` for a schema that isn't the tenant's.
pub async fn fence(
    req: &FenceRequest,
    writer: &NodeEndpoint,
    kill_on: &[NodeEndpoint],
    opts: &FenceOptions,
) -> Result<FenceReceipt> {
    check_schemas(req.tenant, &req.schemas)?;
    if Instant::now() >= req.deadline {
        return Err(fail("the fence deadline passed before it started"));
    }
    let started = Instant::now();
    let r = match req.method {
        FenceMethod::RevokeKill => revoke_kill(req, writer, kill_on, opts).await,
        FenceMethod::Rename => rename(req, writer, opts).await,
    };
    match &r {
        Ok(receipt) => tracing::info!(
            tenant = %req.tenant,
            method = ?req.method,
            scope = ?req.scope,
            revoked = receipt.revoked.len(),
            renamed = receipt.renamed.len(),
            killed = receipt.killed.len(),
            took_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "shard fenced"
        ),
        Err(e) => {
            tracing::warn!(tenant = %req.tenant, method = ?req.method, error = %e, "fence failed");
        }
    }
    r
}

async fn revoke_kill(
    req: &FenceRequest,
    writer: &NodeEndpoint,
    kill_on: &[NodeEndpoint],
    opts: &FenceOptions,
) -> Result<FenceReceipt> {
    let mut s = ProbeSession::connect(writer, opts.timeout)
        .await
        .map_err(|e| fail_with("connect to the writer", e))?;
    let mut receipt = FenceReceipt {
        tenant: req.tenant,
        method: FenceMethod::RevokeKill,
        scope: req.scope,
        writer: writer.node.clone(),
        revoked: Vec::new(),
        renamed: Vec::new(),
        killed: Vec::new(),
        writer_pos: GtidPos::default(),
        fenced_at_unix: None,
    };
    let r = revoke_then_kill(req, &mut s, kill_on, opts, &mut receipt).await;
    if let Err(e) = r {
        // Undo the revokes made so far; kills can't be undone.
        if let Err(undo) = regrant(&mut s, &receipt.revoked).await {
            tracing::error!(tenant = %req.tenant, error = %undo, "undoing a failed fence failed");
        }
        s.close().await;
        return Err(e);
    }
    s.close().await;
    Ok(receipt)
}

async fn revoke_then_kill(
    req: &FenceRequest,
    s: &mut ProbeSession,
    kill_on: &[NodeEndpoint],
    opts: &FenceOptions,
    receipt: &mut FenceReceipt,
) -> Result<()> {
    // 1. The tenant's grants on the schemas.
    let spellings: Vec<String> = req.schemas.iter().flat_map(|s| db_spellings(s)).collect();
    let rows = s
        .rows(&format!(
            "SELECT * FROM mysql.db WHERE User LIKE {} AND Db IN ({})",
            literal(&tenant_account_like(req.tenant)),
            in_list(spellings.iter().map(String::as_str))
        ))
        .await
        .map_err(|e| fail_with("read mysql.db", e))?;
    // 2. Revoke.
    for g in grants_to_revoke(&rows, req.tenant, &req.schemas, req.scope) {
        s.exec(&g.revoke_sql())
            .await
            .map_err(|e| fail_with(&format!("revoke from {}", g.user), e))?;
        receipt.revoked.push(g);
    }
    // 3. Kill the tenant's sessions that could still write, on every node.
    let mut pending: Vec<(usize, Vec<u64>)> = Vec::new();
    let mut sessions = Vec::with_capacity(kill_on.len());
    for (i, ep) in kill_on.iter().enumerate() {
        let mut k = ProbeSession::connect(ep, opts.timeout)
            .await
            .map_err(|e| fail_with(&format!("connect to {}", ep.node), e))?;
        let ids = kill_sessions(&mut k, req, opts).await?;
        receipt
            .killed
            .extend(ids.iter().map(|id| (ep.node.clone(), *id)));
        if !ids.is_empty() {
            pending.push((i, ids));
        }
        sessions.push(k);
    }
    // 4. Wait until they are gone: KILL returns before the thread stops.
    let wait_until = (Instant::now() + opts.kill_wait).min(req.deadline);
    loop {
        let mut still = Vec::new();
        for (i, ids) in &pending {
            let rows = sessions[*i]
                .rows(&format!(
                    "SELECT ID FROM information_schema.PROCESSLIST WHERE ID IN ({})",
                    ids.iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .await
                .map_err(|e| fail_with("poll killed sessions", e))?;
            let left: Vec<u64> = rows
                .iter()
                .filter_map(|r| first_col(r).and_then(|v| v.parse().ok()))
                .collect();
            if !left.is_empty() {
                still.push((*i, left));
            }
        }
        pending = still;
        if pending.is_empty() {
            break;
        }
        if Instant::now() >= wait_until {
            for k in sessions {
                k.close().await;
            }
            let left: Vec<String> = pending
                .iter()
                .flat_map(|(i, ids)| {
                    ids.iter()
                        .map(move |id| format!("{}:{id}", kill_on[*i].node))
                })
                .collect();
            return Err(fail(format!(
                "killed sessions still running after {} ms: {}",
                opts.kill_wait.as_millis(),
                left.join(", ")
            )));
        }
        tokio::time::sleep(KILL_POLL).await;
    }
    for k in sessions {
        k.close().await;
    }
    // 5. The position after which no write to the shard can appear.
    receipt.writer_pos = GtidPos(
        s.scalar("SELECT @@gtid_binlog_pos")
            .await
            .map_err(|e| fail_with("read @@gtid_binlog_pos", e))?
            .unwrap_or_default(),
    );
    Ok(())
}

/// Finds and kills the tenant's sessions on one node; returns their ids.
async fn kill_sessions(
    k: &mut ProbeSession,
    req: &FenceRequest,
    opts: &FenceOptions,
) -> Result<Vec<u64>> {
    let rows = k
        .rows(&format!(
            "SELECT ID, USER, DB, INFO FROM information_schema.PROCESSLIST WHERE USER LIKE {}",
            literal(&tenant_account_like(req.tenant))
        ))
        .await
        .map_err(|e| fail_with("read the process list", e))?;
    let rows: Vec<SessionRow> = rows
        .iter()
        .filter_map(|r| {
            Some(SessionRow {
                id: col(r, "ID")?.parse().ok()?,
                user: col(r, "USER")?.to_owned(),
                db: col(r, "DB").map(str::to_owned),
                info: col(r, "INFO").map(str::to_owned),
            })
        })
        .collect();
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let ids = |rows: Vec<TextRow>| -> HashSet<u64> {
        rows.iter()
            .filter_map(|r| first_col(r).and_then(|v| v.parse().ok()))
            .collect()
    };
    let mdl = if opts.use_performance_schema {
        match k
            .rows(&format!(
                "SELECT DISTINCT t.PROCESSLIST_ID FROM performance_schema.metadata_locks m \
                 JOIN performance_schema.threads t ON t.THREAD_ID = m.OWNER_THREAD_ID \
                 WHERE m.OBJECT_SCHEMA IN ({})",
                in_list(req.schemas.iter().map(String::as_str))
            ))
            .await
        {
            Ok(r) => Some(ids(r)),
            Err(e) if e.is_server_error() => None,
            Err(e) => return Err(fail_with("read metadata locks", e)),
        }
    } else {
        None
    };
    let open_trx = if mdl.is_some() {
        HashSet::new()
    } else {
        ids(k
            .rows("SELECT trx_mysql_thread_id FROM information_schema.INNODB_TRX")
            .await
            .map_err(|e| fail_with("read open transactions", e))?)
    };
    let victims = sessions_to_kill(&rows, req.tenant, &req.schemas, &open_trx, mdl.as_ref());
    for id in &victims {
        match k.exec(&format!("KILL CONNECTION {id}")).await {
            // 1094: unknown thread, already gone.
            Ok(()) => {}
            Err(e) if e.errno == Some(1094) => {}
            Err(e) => return Err(fail_with(&format!("kill session {id}"), e)),
        }
    }
    Ok(victims)
}

async fn regrant(s: &mut ProbeSession, revoked: &[RevokedGrant]) -> Result<()> {
    let mut first_err = None;
    for g in revoked {
        match s.exec(&g.grant_sql()).await {
            Ok(()) => {}
            // 1133: the account was dropped meanwhile; nothing to restore.
            Err(e) if e.errno == Some(1133) => {
                tracing::warn!(user = %g.user, "unfence: account no longer exists");
            }
            Err(e) => {
                tracing::error!(user = %g.user, error = %e, "unfence: grant failed");
                first_err.get_or_insert(e);
            }
        }
    }
    match first_err {
        None => Ok(()),
        Some(e) => Err(SprigError::from(e)),
    }
}

/// `information_schema.TABLES` rows of `schemas`, one `TABLE_SCHEMA =` branch
/// per schema joined by `UNION ALL`. An `IN` list or `LIKE` makes MariaDB
/// open every schema on the server (schemas-per-cluster spike, chapter 07);
/// equality opens only the one named. `schemas` must not be empty.
fn tables_query(schemas: &[String], extra: &str) -> String {
    schemas
        .iter()
        .map(|sc| {
            format!(
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = {}{extra}",
                literal(sc)
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

/// Base tables (with system-versioned tables and sequences) of `schemas`,
/// except those already fenced.
async fn tables_of(
    s: &mut ProbeSession,
    schemas: &[String],
) -> std::result::Result<Vec<(String, String)>, ProbeError> {
    if schemas.is_empty() {
        return Ok(Vec::new());
    }
    let rows = s
        .rows(&format!(
            "{} ORDER BY 1, 2",
            tables_query(
                schemas,
                " AND TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED', 'SEQUENCE')"
            )
        ))
        .await?;
    Ok(rows
        .iter()
        .filter_map(|r| {
            let mut it = r.iter().map(|(_, v)| v.clone());
            Some((it.next()??, it.next()??))
        })
        .collect())
}

/// The `RENAME TABLE` statement for `pairs`, in order.
#[must_use]
pub fn rename_sql(renamed: &[RenamedTable], reverse: bool) -> String {
    let parts: Vec<String> = renamed
        .iter()
        .map(|r| {
            let (a, b) = if reverse {
                (&r.to, &r.from)
            } else {
                (&r.from, &r.to)
            };
            format!(
                "{}.{} TO {}.{}",
                quote(&r.schema),
                quote(a),
                quote(&r.schema),
                quote(b)
            )
        })
        .collect();
    format!("RENAME TABLE {}", parts.join(", "))
}

async fn rename(
    req: &FenceRequest,
    writer: &NodeEndpoint,
    opts: &FenceOptions,
) -> Result<FenceReceipt> {
    let lock_wait = Duration::from_secs(opts.lock_wait_seconds);
    // The rename may wait for metadata-lock holders for the lock wait.
    let mut s = ProbeSession::connect(writer, opts.timeout + lock_wait)
        .await
        .map_err(|e| fail_with("connect to the writer", e))?;
    let r = rename_on(req, &mut s, opts).await;
    s.close().await;
    let (renamed, pos, fenced_at) = r?;
    Ok(FenceReceipt {
        tenant: req.tenant,
        method: FenceMethod::Rename,
        scope: req.scope,
        writer: writer.node.clone(),
        revoked: Vec::new(),
        renamed,
        killed: Vec::new(),
        writer_pos: pos,
        fenced_at_unix: fenced_at,
    })
}

async fn rename_on(
    req: &FenceRequest,
    s: &mut ProbeSession,
    opts: &FenceOptions,
) -> Result<(Vec<RenamedTable>, GtidPos, Option<u64>)> {
    s.exec(&format!(
        "SET SESSION lock_wait_timeout = {}",
        opts.lock_wait_seconds
    ))
    .await
    .map_err(|e| fail_with("set lock_wait_timeout", e))?;
    let all = tables_of(s, &req.schemas)
        .await
        .map_err(|e| fail_with("list tables", e))?;
    let renamed: Vec<RenamedTable> = all
        .into_iter()
        .filter(|(_, t)| !is_fenced_name(t))
        .filter(|(_, t)| req.tables.as_ref().is_none_or(|only| only.contains(t)))
        .map(|(schema, t)| RenamedTable {
            to: fenced_name(&t),
            schema,
            from: t,
        })
        .collect();
    if let Some(only) = &req.tables {
        let missing: Vec<&String> = only
            .iter()
            .filter(|t| !renamed.iter().any(|r| &r.from == *t))
            .collect();
        if !missing.is_empty() && renamed.is_empty() {
            return Err(fail(format!("no such tables to fence: {missing:?}")));
        }
    }
    if renamed.is_empty() {
        let pos = s
            .scalar("SELECT @@gtid_binlog_pos")
            .await
            .map_err(|e| fail_with("read @@gtid_binlog_pos", e))?;
        return Ok((renamed, GtidPos(pos.unwrap_or_default()), None));
    }
    let fenced_at = s
        .scalar("SELECT UNIX_TIMESTAMP()")
        .await
        .map_err(|e| fail_with("read the server time", e))?
        .and_then(|v| v.parse().ok());
    // One atomic statement: it waits for metadata-lock holders, and later
    // statements queue behind it, so no write follows it in the binlog.
    s.exec(&rename_sql(&renamed, false))
        .await
        .map_err(|e| fail_with("rename", e))?;
    let last = s
        .scalar("SELECT @@last_gtid")
        .await
        .map_err(|e| fail_with("read @@last_gtid", e))?
        .filter(|v| !v.is_empty());
    let pos = match last {
        Some(p) => p,
        // Binary logging off for this session: fall back to the server's.
        None => s
            .scalar("SELECT @@gtid_binlog_pos")
            .await
            .map_err(|e| fail_with("read @@gtid_binlog_pos", e))?
            .unwrap_or_default(),
    };
    Ok((renamed, GtidPos(pos), fenced_at))
}

/// Undoes a fence exactly as its receipt says: grants are restored and
/// tables renamed back. Idempotent: a table already back under its name and
/// an account dropped meanwhile are skipped. The caller then asks the grant
/// reconciler (chapter 29) to recheck the tenant.
///
/// # Errors
/// A statement failed; what could be restored was.
pub async fn unfence(
    receipt: &FenceReceipt,
    writer: &NodeEndpoint,
    opts: &FenceOptions,
) -> Result<()> {
    let lock_wait = Duration::from_secs(opts.lock_wait_seconds);
    let mut s = ProbeSession::connect(writer, opts.timeout + lock_wait).await?;
    let r = unfence_on(receipt, &mut s, opts).await;
    s.close().await;
    if r.is_ok() {
        tracing::info!(
            tenant = %receipt.tenant,
            method = ?receipt.method,
            regranted = receipt.revoked.len(),
            renamed = receipt.renamed.len(),
            "shard unfenced"
        );
    }
    r
}

async fn unfence_on(
    receipt: &FenceReceipt,
    s: &mut ProbeSession,
    opts: &FenceOptions,
) -> Result<()> {
    if !receipt.renamed.is_empty() {
        s.exec(&format!(
            "SET SESSION lock_wait_timeout = {}",
            opts.lock_wait_seconds
        ))
        .await?;
        let schemas: Vec<String> = receipt
            .renamed
            .iter()
            .map(|r| r.schema.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let rows = if schemas.is_empty() {
            Vec::new()
        } else {
            s.rows(&tables_query(&schemas, "")).await?
        };
        let present: HashSet<(String, String)> = rows
            .iter()
            .filter_map(|r| {
                let mut it = r.iter().map(|(_, v)| v.clone());
                Some((it.next()??, it.next()??))
            })
            .collect();
        let has = |schema: &str, t: &str| present.contains(&(schema.to_owned(), t.to_owned()));
        let mut back = Vec::new();
        let mut strays = Vec::new();
        for r in &receipt.renamed {
            match (has(&r.schema, &r.to), has(&r.schema, &r.from)) {
                (true, false) => back.push(r.clone()),
                (false, true) => {} // already back
                (true, true) => strays.push(r.clone()),
                (false, false) => {
                    tracing::warn!(schema = %r.schema, table = %r.from, "unfence: table is gone");
                }
            }
        }
        if !strays.is_empty() {
            drop_strays(s, &strays, receipt.fenced_at_unix).await?;
            back.extend(strays);
        }
        if !back.is_empty() {
            s.exec(&rename_sql(&back, true)).await?;
        }
    }
    regrant(s, &receipt.revoked).await
}

/// Both a renamed table and a table under its old name exist: something
/// recreated the table while the fence held (catalog migrations did, on a
/// fenced catalog: chapter 10, "Cutover"). The recreation is dropped only
/// if it is provably a stray: created at or after the fence
/// (`fenced_at`, the writer's clock) and empty, checked under a write lock
/// so nothing can fill it in between. Otherwise nothing is dropped and the
/// unfence fails, naming each table and why: an operator decides.
async fn drop_strays(
    s: &mut ProbeSession,
    strays: &[RenamedTable],
    fenced_at: Option<u64>,
) -> Result<()> {
    let name = |r: &RenamedTable| format!("{}.{}", quote(&r.schema), quote(&r.from));
    let conflict = |why: String| {
        SprigError::new(
            SprigCode::ClusterFenceFailed,
            format!(
                "unfence: both a table and its fenced name exist, and the table isn't provably a \
                 stray recreation ({why}); nothing was dropped: compare them, drop the wrong one \
                 and retry"
            ),
        )
    };
    let Some(fenced_at) = fenced_at else {
        let names: Vec<String> = strays.iter().map(name).collect();
        return Err(conflict(format!(
            "{}: the fence receipt has no fence time",
            names.join(", ")
        )));
    };
    let locks: Vec<String> = strays
        .iter()
        .map(|r| format!("{} WRITE", name(r)))
        .collect();
    s.exec(&format!("LOCK TABLES {}", locks.join(", "))).await?;
    let checked = async {
        let mut why = Vec::new();
        for r in strays {
            let created = s
                .scalar(&format!(
                    "SELECT UNIX_TIMESTAMP(CREATE_TIME) FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                    literal(&r.schema),
                    literal(&r.from)
                ))
                .await?
                .and_then(|v| v.split('.').next().and_then(|n| n.parse::<u64>().ok()));
            let has_rows = s
                .scalar(&format!("SELECT 1 FROM {} LIMIT 1", name(r)))
                .await?
                .is_some();
            match created {
                _ if has_rows => why.push(format!("{} has rows", name(r))),
                Some(c) if c >= fenced_at => {}
                Some(c) => why.push(format!(
                    "{} was created at {c}, before the fence at {fenced_at}",
                    name(r)
                )),
                None => why.push(format!("{} has no creation time", name(r))),
            }
        }
        Ok::<_, ProbeError>(why)
    }
    .await;
    let r = match checked {
        Ok(why) if why.is_empty() => {
            let names: Vec<String> = strays.iter().map(name).collect();
            let dropped = s.exec(&format!("DROP TABLE {}", names.join(", "))).await;
            if dropped.is_ok() {
                tracing::warn!(tables = ?names, fenced_at,
                    "unfence: dropped empty tables recreated under fenced names after the fence");
            }
            dropped.map_err(SprigError::from)
        }
        Ok(why) => Err(conflict(why.join("; "))),
        Err(e) => Err(SprigError::from(e)),
    };
    let unlocked = s.exec("UNLOCK TABLES").await;
    r?;
    unlocked.map_err(SprigError::from)
}

#[cfg(test)]
mod tests;
