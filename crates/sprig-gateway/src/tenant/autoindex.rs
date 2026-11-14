//! The gateway's side of hiding automatic indexes from metadata, next to
//! the hiding in `sprig-sql`:
//!
//! - [`session_flag`]: `SET @sprig_show_auto_indexes = 1|0` is kept in the
//!   session ([`TenantSession::show_auto_indexes`]) and answered with OK;
//!   it never reaches MariaDB. Plans then use
//!   [`TenantSession::visibility`], so `SHOW INDEX`, `SHOW CREATE TABLE`
//!   and `information_schema.STATISTICS` show automatic indexes, and the
//!   session may `DROP INDEX sprig_auto_...`.
//! - [`make_room`]: the 64-index rule. Before a user statement that adds
//!   indexes to a table already holding 64 or close to it, the gateway
//!   drops the lowest-value automatic indexes on that table (as
//!   `sprig_ctl`, `DROP INDEX IF EXISTS`) and marks their catalog rows
//!   `dropped` with reason `index_limit`. Values are
//!   `est_benefit_ms_h - est_write_ms_h` from the tenant's `auto_indexes`
//!   (the table is `sprig-advisor`'s `autoindex` module, read here with
//!   plain SQL so the gateway doesn't depend on the advisor); an index no
//!   row explains goes first. When dropping every automatic index wouldn't
//!   make room, nothing is dropped and MariaDB refuses the statement with
//!   1069. Failures are logged and never block the user's statement.

use mysql_async::prelude::Queryable;
use mysql_async::{Pool, Row};
use sprig_catalog::CatalogLocator;
use sprig_catalog::store::Namespace;
use sprig_common::Result;
use sprig_common::names::{is_auto_index, quote};
use sprig_sql::lex::LexOptions;
use sprig_sql::names::{
    DdlKind, DdlTarget, ExistingIndex, MAX_INDEXES_PER_TABLE, NameMap, Purpose, index_changes,
};

use super::session::TenantSession;

/// `SET @sprig_show_auto_indexes = <v>`: applies it to `ts`. `None` when
/// the statement is anything else.
///
/// # Errors
/// 39051 for the variable set together with others or to a value that
/// isn't `1`, `0`, `ON`, `OFF`, `TRUE` or `FALSE`.
pub fn session_flag(ts: &mut TenantSession, text: &[u8], lex: &LexOptions) -> Option<Result<()>> {
    let r = sprig_sql::names::session_flag(text, lex)?;
    Some(match r {
        Ok(v) => {
            ts.show_auto_indexes = v;
            Ok(())
        }
        Err(e) => Err(e.into()),
    })
}

/// The 64-index rule for one DDL statement (module docs). Returns the
/// automatic indexes dropped.
pub async fn make_room(
    ts: &TenantSession,
    ctl: &Pool,
    target: &DdlTarget,
    original: &[u8],
    lex: &LexOptions,
) -> Vec<String> {
    let DdlTarget::AlterObject {
        kind: DdlKind::Table,
        name,
        ..
    } = target
    else {
        return Vec::new();
    };
    let adding = index_changes(original, lex).map_or(0, |c| c.net_added());
    let Ok(adding) = usize::try_from(adding) else {
        return Vec::new();
    };
    if adding == 0 {
        return Vec::new();
    }
    let Some(db) = name.db.as_deref() else {
        return Vec::new();
    };
    match room(ts, ctl, db, &name.name, adding).await {
        Ok(dropped) => dropped,
        Err(e) => {
            tracing::warn!(error = %e, db, table = %name.name,
                "the 64-index check failed; the statement runs as is");
            Vec::new()
        }
    }
}

async fn room(
    ts: &TenantSession,
    ctl: &Pool,
    db: &str,
    table: &str,
    adding: usize,
) -> Result<Vec<String>> {
    let map = ts.map();
    let Ok(schemas) = map.physical(db, Purpose::Ddl) else {
        return Ok(Vec::new());
    };
    let mut conn = ctl.get_conn().await.map_err(backend)?;
    let mut dropped = Vec::new();
    for schema in &schemas.0 {
        let schema = schema.to_string();
        let names: Vec<String> = conn
            .exec(
                "SELECT INDEX_NAME FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? GROUP BY INDEX_NAME",
                (&schema, table),
            )
            .await
            .map_err(backend)?;
        if names.len() + adding <= MAX_INDEXES_PER_TABLE || !names.iter().any(|n| is_auto_index(n))
        {
            continue;
        }
        let values = values(ts, db, table).await.unwrap_or_else(|e| {
            tracing::warn!(error = %e, "auto_indexes unreadable; every automatic index counts as worthless");
            Vec::new()
        });
        let existing: Vec<ExistingIndex> = names
            .iter()
            .map(|n| ExistingIndex {
                name: n.clone(),
                value: values
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(n))
                    .map(|(_, v)| *v),
            })
            .collect();
        for x in sprig_sql::names::make_room(&existing, adding) {
            conn.query_drop(format!(
                "ALTER TABLE {}.{} DROP INDEX IF EXISTS {}",
                quote(&schema),
                quote(table),
                quote(&x)
            ))
            .await
            .map_err(backend)?;
            tracing::info!(event = "decision", component = "gateway", kind = "index.drop",
                tenant = %ts.tenant, reason_code = "index_limit", index = %x, table,
                "dropped an automatic index to make room under MariaDB's 64-index limit");
            if !dropped.contains(&x) {
                dropped.push(x);
            }
        }
    }
    if !dropped.is_empty()
        && let Err(e) = mark_dropped(ts, db, table, &dropped).await
    {
        tracing::warn!(error = %e, "auto_indexes rows not marked; the reconciler will see the drift");
    }
    Ok(dropped)
}

/// The object id of `db`.`table` in the session's snapshot.
fn object_id(ts: &TenantSession, db: &str, table: &str) -> Option<u64> {
    let cat = ts.catalog()?;
    let d = cat
        .databases
        .by_name(db)
        .or_else(|| cat.databases.by_name(&db.to_lowercase()))?;
    let o = cat
        .objects
        .by_name(d.db_id, Namespace::Relation, table)
        .or_else(|| {
            cat.objects
                .by_name(d.db_id, Namespace::Relation, &table.to_lowercase())
        })?;
    Some(o.object_id.0)
}

async fn catalog_conn(ts: &TenantSession) -> Result<(mysql_async::Conn, String)> {
    let ep = ts.services().locator.writer(ts.tenant).await?;
    let conn = ep.pool.get_conn().await.map_err(backend)?;
    Ok((conn, ep.store.schema().to_owned()))
}

/// `(name, est_benefit_ms_h - est_write_ms_h)` of the table's live
/// automatic indexes; empty without a catalog row for the table or before
/// the `autoindex` module is migrated.
async fn values(ts: &TenantSession, db: &str, table: &str) -> Result<Vec<(String, f64)>> {
    let Some(object) = object_id(ts, db, table) else {
        return Ok(Vec::new());
    };
    let (mut conn, schema) = catalog_conn(ts).await?;
    let r: std::result::Result<Vec<Row>, _> = conn
        .exec(
            format!(
                "SELECT name, est_benefit_ms_h - est_write_ms_h FROM {}.auto_indexes \
                 WHERE object_id = ? AND state NOT IN ('dropped', 'rejected')",
                quote(&schema)
            ),
            (object,),
        )
        .await;
    match r {
        Ok(rows) => Ok(rows
            .into_iter()
            .filter_map(|mut r| Some((r.take::<String, _>(0)?, r.take::<f64, _>(1)?)))
            .collect()),
        // 1146: the module isn't migrated in this catalog yet.
        Err(mysql_async::Error::Server(e)) if e.code == 1146 => Ok(Vec::new()),
        Err(e) => Err(backend(e)),
    }
}

async fn mark_dropped(ts: &TenantSession, db: &str, table: &str, names: &[String]) -> Result<()> {
    let Some(object) = object_id(ts, db, table) else {
        return Ok(());
    };
    let (mut conn, schema) = catalog_conn(ts).await?;
    for n in names {
        let r = conn
            .exec_drop(
                format!(
                    "UPDATE {}.auto_indexes SET state = 'dropped', prior_state = NULL, \
                     drop_reason = 'index_limit', state_changed_at = UTC_TIMESTAMP(6) \
                     WHERE object_id = ? AND name = ? AND state NOT IN ('dropped', 'rejected')",
                    quote(&schema)
                ),
                (object, n),
            )
            .await;
        match r {
            Ok(()) => {}
            Err(mysql_async::Error::Server(e)) if e.code == 1146 => return Ok(()),
            Err(e) => return Err(backend(e)),
        }
    }
    Ok(())
}

fn backend(e: mysql_async::Error) -> sprig_common::SprigError {
    sprig_common::SprigError::new(
        sprig_common::SprigCode::BackendUnavailable,
        "the 64-index check couldn't reach MariaDB",
    )
    .with_source(e)
}
