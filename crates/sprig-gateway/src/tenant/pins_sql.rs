//! Sharding pins and advice in SQL, executed (chapter 16, "Interfaces";
//! build steps 7 and 8): [`sprig_sql::pin_stmt`] parses, this resolves
//! names against the session's snapshot, checks privileges and the
//! conflict rules, and reads or writes the tenant's catalog.
//!
//! - **`SPRIG PIN t ...`** needs `ALTER` on `t`, and for `COLOCATE WITH p`
//!   on `p` too (1142, chapter 29's check). An unqualified `p` is in `t`'s
//!   database. Inside one catalog transaction the gateway reads every pin
//!   `FOR UPDATE` (so two pin statements check one after the other), the
//!   current group members, the named tables' columns from
//!   `information_schema` on the catalog's cluster (the tenant's cluster
//!   in R1), and the shard count of `t`'s group from its shard map; then
//!   [`sprig_shard::pins::check_pin`] applies the conflict rules (39410,
//!   1072) and the split-group rule (39411); then the pin is written to
//!   `group_pins` (replacing an earlier pin of `t`) and
//!   `group_members.pinned` follows whether `t` already sits where the pin
//!   says. The `sharding` module is migrated first, so a catalog from
//!   before format 3 gets the table.
//! - **`SPRIG UNPIN t`** needs `ALTER` on `t`; it deletes the row (OK with
//!   0 rows when there was none) and clears `group_members.pinned`.
//! - **`SPRIG SHOW ADVICE`** answers the newest `advisor_runs` row: the
//!   run's columns on every row, and one row per design member (group,
//!   database, table, routing, key columns, parent, pin), then one per
//!   table the design leaves unsharded. Names are logical: the design
//!   stores table names and object ids, never schemas. Rows for tables
//!   the session has no privilege on are left out. No run: no rows.
//! - **`SPRIG SHOW PINS`** lists the pins of tables the session can see.
//!
//! Pins take effect in the next first design (chapter 16, "As built in
//! R1"): a tenant whose groups exist already keeps them until R3's
//! periodic runs and re-keys.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use mysql_async::prelude::Queryable;
use mysql_async::{Row, TxOpts};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DatabaseRow, DbState, Namespace, ObjectKind, ObjectRow, ObjectState};
use sprig_catalog::{CatalogLocator, Target, TenantCatalog};
use sprig_common::ids::{DbId, GroupId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Tier, TierView};
use sprig_proto_mysql::ColumnDef;
use sprig_shard::ShardMapStore;
use sprig_shard::pins::{
    Columns, Facts, Pin, PinKind, PinStore, check_pin, column_class, matches_placement,
};
use sprig_sql::names::LocalAnswer;
use sprig_sql::pin_stmt::{PinSpec, PinStmt, PinTable};

use super::TenantServices;
use super::session::TenantSession;
use crate::pipeline::{int_column, text_column};

/// What a pin statement answers.
#[derive(Debug)]
pub enum PinAnswer {
    /// OK with this many rows affected.
    Ok {
        /// Rows: 1 for a pin, 0 or 1 for an unpin.
        affected: u64,
    },
    /// A resultset (`SHOW ADVICE`, `SHOW PINS`).
    Rows(LocalAnswer),
}

fn no_database() -> SprigError {
    SprigError::standard(1046, *b"3D000", "No database selected")
}

fn backend(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("pins: the catalog's cluster: {e}"),
    )
    .with_source(e)
}

fn missing_table(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1146)
}

/// `[db.]name` against `cat`: the database (1046, 1049) and the table
/// (1146), active ones only.
fn resolve(
    ts: &TenantSession,
    cat: &TenantCatalog,
    db: Option<&str>,
    name: &str,
) -> Result<(Arc<DatabaseRow>, Arc<ObjectRow>)> {
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
            &object_key(ObjectKind::Table, name, lctn),
        )
        .filter(|o| o.state == ObjectState::Active)
        .ok_or_else(|| {
            SprigError::standard(
                1146,
                *b"42S02",
                format!("Table '{dbn}.{name}' doesn't exist"),
            )
        })?;
    Ok((Arc::clone(d), Arc::clone(o)))
}

/// `ALTER` on the table (chapter 29, as `ALTER TABLE ... SPRIG SET`).
fn need_alter(ts: &TenantSession, d: &DatabaseRow, o: &ObjectRow) -> Result<()> {
    super::authz::settings_check(
        ts,
        Target::Object(o.object_id),
        false,
        false,
        false,
        Some(&d.name),
        Some(&o.name),
        sprig_proto_mysql::Persona::MariaDb,
    )
}

/// `db.table` of object `o` in `cat`, logical; `None` for one it lacks.
fn logical(cat: &TenantCatalog, o: ObjectId) -> Option<(Arc<DatabaseRow>, Arc<ObjectRow>)> {
    let obj = cat.objects.get(o)?;
    let db = cat.databases.get(obj.db_id)?;
    Some((Arc::clone(db), Arc::clone(obj)))
}

fn display(cat: &TenantCatalog, o: ObjectId) -> String {
    logical(cat, o).map_or_else(
        || format!("object {o}"),
        |(d, t)| format!("{}.{}", d.name, t.name),
    )
}

fn visible(ts: &TenantSession, cat: &TenantCatalog, o: ObjectId) -> bool {
    logical(cat, o).is_some_and(|(d, t)| {
        super::authz::settings_visible(ts, cat, &d.name, d.db_id, Some(t.object_id))
    })
}

/// The columns of `objects` from `information_schema` on the connection's
/// server: per table, the first of its database's shard schemas holding
/// it. A table found nowhere is left out (its rules are skipped).
async fn read_columns<Q: Queryable + Send>(
    c: &mut Q,
    tenant: TenantId,
    cat: &TenantCatalog,
    objects: &BTreeSet<ObjectId>,
) -> Result<BTreeMap<ObjectId, Columns>> {
    let mut by_db: BTreeMap<DbId, Vec<(ObjectId, String)>> = BTreeMap::new();
    for o in objects {
        if let Some((d, t)) = logical(cat, *o) {
            by_db.entry(d.db_id).or_default().push((*o, t.name.clone()));
        }
    }
    let mut out = BTreeMap::new();
    for (db, tables) in by_db {
        let marks = vec!["?"; tables.len()].join(", ");
        let mut params: Vec<mysql_async::Value> = vec![format!("t{tenant}\\_d{db}\\_%").into()];
        params.extend(tables.iter().map(|(_, n)| n.clone().into()));
        let rows: Vec<(String, String, String, String, Option<String>)> = c
            .exec(
                format!(
                    "SELECT TABLE_SCHEMA, TABLE_NAME, LOWER(COLUMN_NAME), DATA_TYPE, \
                     COLLATION_NAME FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA LIKE ? AND TABLE_NAME IN ({marks}) \
                     ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
                ),
                params,
            )
            .await
            .map_err(backend)?;
        for (o, name) in tables {
            let Some(schema) = rows
                .iter()
                .find(|r| r.1.eq_ignore_ascii_case(&name))
                .map(|r| r.0.clone())
            else {
                continue;
            };
            let cols: Columns = rows
                .iter()
                .filter(|r| r.0 == schema && r.1.eq_ignore_ascii_case(&name))
                .map(|r| (r.2.clone(), column_class(&r.3, r.4.as_deref())))
                .collect();
            out.insert(o, cols);
        }
    }
    Ok(out)
}

/// Shards in group `g`'s map, in the tenant's home DC; 0 without a map.
async fn shard_count(svc: &TenantServices, tenant: TenantId, g: GroupId) -> Result<usize> {
    let home = svc.locator.home_of(tenant);
    let dc = svc.locator.dc_store();
    let store = if home == svc.dc {
        ShardMapStore::new(dc)
    } else {
        ShardMapStore::new(TierView::over(&dc, Tier::Dc(home)))
    };
    match store.load(tenant, g).await {
        Ok((m, _)) => Ok(m
            .ranges
            .iter()
            .map(|r| r.shard)
            .collect::<BTreeSet<_>>()
            .len()),
        Err(e) if e.code() == Some(SprigCode::ShardMapUnavailable) => Ok(0),
        Err(e) => Err(e),
    }
}

fn lower(cols: &[String]) -> Vec<String> {
    cols.iter().map(|c| c.to_lowercase()).collect()
}

/// Runs `stmt` for the session. `cat` is the snapshot the statement reads;
/// `current` the session's logical database.
///
/// # Errors
/// As the module docs list: 1046, 1049, 1146, 1142, 1072, 39410, 39411,
/// and the catalog cluster's errors.
pub async fn execute(
    ts: &mut TenantSession,
    cat: &Arc<TenantCatalog>,
    current: Option<&str>,
    stmt: &PinStmt,
    charset: u16,
) -> Result<PinAnswer> {
    match stmt {
        PinStmt::Pin { table, spec } => pin(ts, cat, current, table, spec).await,
        PinStmt::Unpin { table } => unpin(ts, cat, current, table).await,
        PinStmt::ShowAdvice => show_advice(ts, cat, charset).await,
        PinStmt::ShowPins => show_pins(ts, cat, charset).await,
    }
}

async fn pin(
    ts: &TenantSession,
    cat: &Arc<TenantCatalog>,
    current: Option<&str>,
    table: &PinTable,
    spec: &PinSpec,
) -> Result<PinAnswer> {
    let (d, o) = resolve(ts, cat, table.db.as_deref().or(current), &table.name)?;
    need_alter(ts, &d, &o)?;
    let kind = match spec {
        PinSpec::ShardKey(cols) => PinKind::ShardKey {
            columns: lower(cols),
        },
        PinSpec::Colocate { parent, columns } => {
            let (pd, po) = resolve(
                ts,
                cat,
                Some(parent.db.as_deref().unwrap_or(&d.name)),
                &parent.name,
            )?;
            need_alter(ts, &pd, &po)?;
            PinKind::Colocate {
                parent: po.object_id,
                columns: lower(columns),
            }
        }
        PinSpec::Reference => PinKind::Reference,
        PinSpec::Unsharded => PinKind::Unsharded,
    };
    let new = Pin {
        object: o.object_id,
        db: d.db_id,
        kind,
    };
    let svc = Arc::clone(ts.services());
    let ep = svc.locator.writer(ts.tenant).await?;
    let schema = ep.store.schema().to_owned();
    let mut conn = ep.pool.get_conn().await.map_err(backend)?;
    sprig_shard::groups::migrate(&mut conn, &schema).await?;
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .await
        .map_err(backend)?;
    let pins: Vec<Pin> = PinStore
        .rows(&mut tx, &schema, true)
        .await?
        .into_iter()
        .map(|r| r.pin)
        .collect();
    let mut placed = PinStore.placements(&mut tx, &schema).await?;
    if let Some(p) = placed.iter_mut().find(|p| p.object == new.object) {
        p.shards = shard_count(&svc, ts.tenant, p.group).await?;
    }
    let mut named: BTreeSet<ObjectId> = BTreeSet::new();
    for p in pins.iter().chain(std::iter::once(&new)) {
        named.insert(p.object);
        if let PinKind::Colocate { parent, .. } = &p.kind {
            named.insert(*parent);
        }
    }
    let columns = read_columns(&mut tx, ts.tenant, cat, &named).await?;
    let name = |x: ObjectId| display(cat, x);
    check_pin(
        &new,
        &Facts {
            pins: &pins,
            columns: &columns,
            placed: &placed,
            name: &name,
        },
    )?;
    let by = format!("{}@{}", ts.principal.user, ts.principal.host);
    PinStore.put(&mut tx, &schema, &new, &by).await?;
    PinStore
        .mark_member(
            &mut tx,
            &schema,
            new.object,
            matches_placement(&new, &placed),
        )
        .await?;
    tx.commit().await.map_err(backend)?;
    tracing::info!(event = "decision", component = "gateway", kind = "pin.set",
        tenant = %ts.tenant, object = %new.object, pin = new.kind.as_str(),
        "sharding pin set");
    Ok(PinAnswer::Ok { affected: 1 })
}

async fn unpin(
    ts: &TenantSession,
    cat: &Arc<TenantCatalog>,
    current: Option<&str>,
    table: &PinTable,
) -> Result<PinAnswer> {
    let (d, o) = resolve(ts, cat, table.db.as_deref().or(current), &table.name)?;
    need_alter(ts, &d, &o)?;
    let ep = ts.services().locator.writer(ts.tenant).await?;
    let schema = ep.store.schema().to_owned();
    let mut conn = ep.pool.get_conn().await.map_err(backend)?;
    let mut tx = conn
        .start_transaction(TxOpts::default())
        .await
        .map_err(backend)?;
    let removed = PinStore.remove(&mut tx, &schema, o.object_id).await?;
    PinStore
        .mark_member(&mut tx, &schema, o.object_id, false)
        .await?;
    tx.commit().await.map_err(backend)?;
    if removed {
        tracing::info!(event = "decision", component = "gateway", kind = "pin.clear",
            tenant = %ts.tenant, object = %o.object_id, "sharding pin removed");
    }
    Ok(PinAnswer::Ok {
        affected: u64::from(removed),
    })
}

/// The newest `advisor_runs` row, as `SPRIG SHOW ADVICE` reads it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AdviceRun {
    /// `run_id`.
    pub run_id: u64,
    /// `cause`.
    pub cause: String,
    /// `mode`.
    pub mode: String,
    /// `status`.
    pub status: String,
    /// `verdict`.
    pub verdict: Option<String>,
    /// `best_score`.
    pub best: Option<f64>,
    /// `runner_up_score`.
    pub runner_up: Option<f64>,
    /// `confidence`.
    pub confidence: Option<f64>,
    /// `units_sampled`.
    pub units: u64,
    /// `decision_id`.
    pub decision: Option<u64>,
    /// `created_at`, UTC.
    pub created_at: String,
    /// `design`.
    pub design: Option<serde_json::Value>,
}

/// `SPRIG SHOW ADVICE`'s columns.
#[must_use]
pub fn advice_columns(charset: u16) -> Vec<ColumnDef> {
    let mut cols = vec![int_column("RUN_ID", true)];
    cols.extend(
        [
            "CAUSE",
            "MODE",
            "STATUS",
            "VERDICT",
            "BEST_SCORE",
            "RUNNER_UP_SCORE",
            "CONFIDENCE",
            "UNITS_SAMPLED",
            "DECISION_ID",
            "CREATED_AT",
            "GROUP_NAME",
            "GROUP_KEY",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "ROUTING",
            "KEY_COLUMNS",
            "PARENT_TABLE",
            "PIN",
        ]
        .iter()
        .map(|n| text_column(n, charset)),
    );
    cols
}

type TextRow = Vec<Option<Bytes>>;

#[allow(clippy::unnecessary_wraps)] // for `and_then` chains
fn text(s: &str) -> Option<Bytes> {
    Some(Bytes::copy_from_slice(s.as_bytes()))
}

/// What [`advice_rows`] asks about a design's objects: its logical
/// database and table names, when the session may see it.
pub type Lookup<'a> = &'a dyn Fn(u64) -> Option<(String, String)>;

/// `SPRIG SHOW ADVICE`'s rows for `run` (see the module docs). `see`
/// names an object the session may see, `None` for one it can't (or the
/// catalog lacks); such rows are left out.
#[must_use]
pub fn advice_rows(run: &AdviceRun, see: Lookup<'_>) -> Vec<TextRow> {
    let score = |v: Option<f64>, digits: usize| v.and_then(|x| text(&format!("{x:.digits$}")));
    let head: TextRow = vec![
        text(&run.run_id.to_string()),
        text(&run.cause),
        text(&run.mode),
        text(&run.status),
        run.verdict.as_deref().and_then(text),
        score(run.best, 3),
        score(run.runner_up, 3),
        score(run.confidence, 2),
        text(&run.units.to_string()),
        run.decision.and_then(|d| text(&d.to_string())),
        text(&run.created_at),
    ];
    let row = |tail: [Option<Bytes>; 8]| -> TextRow {
        let mut r = head.clone();
        r.extend(tail);
        r
    };
    let empty = || row(Default::default());
    let Some(design) = &run.design else {
        return vec![empty()];
    };
    let obj = |v: &serde_json::Value| v.get("object").and_then(serde_json::Value::as_u64);
    let pin = |v: &serde_json::Value| v.get("pin").and_then(|p| p.as_str()).and_then(text);
    let mut rows = Vec::new();
    let groups = design["groups"].as_array().map_or(&[][..], Vec::as_slice);
    for g in groups {
        let members = g["members"].as_array().map_or(&[][..], Vec::as_slice);
        let root_seen = members.first().and_then(obj).and_then(see).is_some();
        let group_name = if root_seen {
            g["name"].as_str().and_then(text)
        } else {
            None
        };
        let key = g["key"]["parts"].as_array().map(|parts| {
            parts
                .iter()
                .map(|p| match (p["class"].as_str(), p["collation"].as_str()) {
                    (Some(c), Some(coll)) => format!("{c}({coll})"),
                    (Some(c), None) => c.to_owned(),
                    _ => "?".to_owned(),
                })
                .collect::<Vec<_>>()
                .join(", ")
        });
        for m in members {
            let Some((db, table)) = obj(m).and_then(see) else {
                continue;
            };
            let r = &m["routing"];
            let kind = r["kind"].as_str().unwrap_or("?");
            let cols = r["columns"].as_array().map(|cs| {
                cs.iter()
                    .filter_map(|c| c.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            });
            let parent = r["parent"]
                .as_u64()
                .and_then(see)
                .map(|(pdb, pt)| if pdb == db { pt } else { format!("{pdb}.{pt}") });
            rows.push(row([
                group_name.clone(),
                key.as_deref().and_then(text),
                text(&db),
                text(&table),
                text(kind),
                cols.as_deref().and_then(text),
                parent.as_deref().and_then(text),
                pin(m),
            ]));
        }
    }
    let unsharded = design["unsharded"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    for u in unsharded {
        let Some((db, table)) = obj(u).and_then(see) else {
            continue;
        };
        rows.push(row([
            None,
            None,
            text(&db),
            text(&table),
            text("unsharded"),
            None,
            None,
            pin(u),
        ]));
    }
    if rows.is_empty() {
        rows.push(empty());
    }
    rows
}

async fn show_advice(
    ts: &TenantSession,
    cat: &Arc<TenantCatalog>,
    charset: u16,
) -> Result<PinAnswer> {
    let columns = advice_columns(charset);
    let ep = ts.services().locator.writer(ts.tenant).await?;
    let schema = ep.store.schema().replace('`', "``");
    let mut conn = ep.pool.get_conn().await.map_err(backend)?;
    let r: std::result::Result<Option<Row>, _> = conn
        .query_first(format!(
            "SELECT run_id, cause, mode, status, verdict, best_score, runner_up_score, \
             confidence, units_sampled, decision_id, \
             DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s'), CAST(design AS CHAR) \
             FROM `{schema}`.advisor_runs ORDER BY run_id DESC LIMIT 1"
        ))
        .await;
    let row = match r {
        Ok(Some(row)) => row,
        Ok(None) => {
            return Ok(PinAnswer::Rows(LocalAnswer {
                columns,
                rows: Vec::new(),
            }));
        }
        Err(e) if missing_table(&e) => {
            return Ok(PinAnswer::Rows(LocalAnswer {
                columns,
                rows: Vec::new(),
            }));
        }
        Err(e) => return Err(backend(e)),
    };
    let run = run_from_row(row)?;
    let see = |o: u64| -> Option<(String, String)> {
        let o = ObjectId(o);
        if !visible(ts, cat, o) {
            return None;
        }
        logical(cat, o).map(|(d, t)| (d.name.clone(), t.name.clone()))
    };
    Ok(PinAnswer::Rows(LocalAnswer {
        columns,
        rows: advice_rows(&run, &see),
    }))
}

fn run_from_row(mut r: Row) -> Result<AdviceRun> {
    let e = || SprigError::internal("unreadable advisor_runs row");
    let design: Option<String> = r.take(11).ok_or_else(e)?;
    Ok(AdviceRun {
        run_id: r.take(0).ok_or_else(e)?,
        cause: r.take(1).ok_or_else(e)?,
        mode: r.take(2).ok_or_else(e)?,
        status: r.take(3).ok_or_else(e)?,
        verdict: r.take(4).ok_or_else(e)?,
        best: r.take(5).ok_or_else(e)?,
        runner_up: r.take(6).ok_or_else(e)?,
        confidence: r.take(7).ok_or_else(e)?,
        units: r.take(8).ok_or_else(e)?,
        decision: r.take(9).ok_or_else(e)?,
        created_at: r.take(10).ok_or_else(e)?,
        design: design.and_then(|d| serde_json::from_str(&d).ok()),
    })
}

/// `SPRIG SHOW PINS`'s columns.
#[must_use]
pub fn pins_columns(charset: u16) -> Vec<ColumnDef> {
    [
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "PIN",
        "KEY_COLUMNS",
        "PARENT_SCHEMA",
        "PARENT_TABLE",
        "PINNED_BY",
        "PINNED_AT",
    ]
    .iter()
    .map(|n| text_column(n, charset))
    .collect()
}

async fn show_pins(
    ts: &TenantSession,
    cat: &Arc<TenantCatalog>,
    charset: u16,
) -> Result<PinAnswer> {
    let ep = ts.services().locator.writer(ts.tenant).await?;
    let schema = ep.store.schema().to_owned();
    let mut conn = ep.pool.get_conn().await.map_err(backend)?;
    let pins = PinStore.rows(&mut conn, &schema, false).await?;
    let mut rows = Vec::new();
    for r in pins {
        let p = &r.pin;
        if !visible(ts, cat, p.object) {
            continue;
        }
        let Some((d, t)) = logical(cat, p.object) else {
            continue;
        };
        let cols = p.kind.columns();
        let parent = match &p.kind {
            PinKind::Colocate { parent, .. } => logical(cat, *parent),
            _ => None,
        };
        rows.push(vec![
            text(&d.name),
            text(&t.name),
            text(&p.kind.as_str().replace('_', " ").to_uppercase()),
            (!cols.is_empty()).then(|| Bytes::from(cols.join(", "))),
            parent.as_ref().and_then(|(pd, _)| text(&pd.name)),
            parent.as_ref().and_then(|(_, pt)| text(&pt.name)),
            r.by.as_deref().and_then(text),
            text(&r.at),
        ]);
    }
    Ok(PinAnswer::Rows(LocalAnswer {
        columns: pins_columns(charset),
        rows,
    }))
}

#[cfg(test)]
mod tests;
