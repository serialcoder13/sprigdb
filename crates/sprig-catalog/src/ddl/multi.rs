//! Multi-shard DDL jobs: one statement applied to every
//! shard of a table group, all or nothing.
//!
//! The single-shard engine's three phases stay; phase 2 fans out:
//!
//! ```text
//! prepare: marks + intent row; the intent also records the shard set
//!          (template schema, physical statement, every shard and its cluster)
//! apply:   read the old definition on the first shard (and, for ALTER TABLE,
//!          refuse with 39483 if the shards already disagree)
//!          for each shard in order:
//!            run the statement (ALTER TABLE: with ALGORITHM named, cheapest
//!            first, the method a previous shard needed tried first)
//!            refused on the first shard   → roll back the marks; the error
//!            refused on a later shard     → undo the shards already changed
//!                                           (inverse statements), roll back
//!                                           the marks; the shard's error
//!              … unless the change removed data (DROP, DROP COLUMN): retry
//!                that shard; still failing → 39480, the reconciler finishes
//!            outcome unknown              → leave the job to the reconciler
//! finish:  SHOW CREATE from the first shard, published in one versioned
//!          change (the new version is visible only once every shard has it)
//! ```
//!
//! Recovery ([`DdlEngine::resolve`] for an intent with a shard set) judges
//! each shard by inspection against the catalog's visible definition:
//! every shard changed → publish; none → roll back the marks; some →
//! undo the changed shards (inverse statements), or, when the change
//! removed data, run it on the rest and publish. Either way every shard
//! ends with the same definition and the catalog matches it.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, JobId, TenantId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigCode, SprigError};

use super::backend::{DdlBackend, DdlTx};
use super::exec::{DdlExecutor, Effect, PhysicalDdl, ShardTarget, drop_sql, outcome_unknown};
use super::repair::Resolution;
use super::shards::{self, Method, Undo, comparable, for_shard};
use super::{
    DdlEngine, DdlIntent, DdlStep, FactTarget, Facts, IntentOp, Plan, Planned, errno, logical_text,
    map_names,
};
use crate::store::{DatabaseRow, ObjectKind, ObjectRow, Snapshot};
use crate::version::Locked;

/// Retries of a shard's statement in a job that can only roll forward.
const FORWARD_RETRIES: u32 = 3;

/// One shard of a job: its schema and, when known, its cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRef {
    /// The physical schema.
    pub schema: String,
    /// Its cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
}

/// The shards a multi-shard job changes, recorded in its intent row so
/// the reconciler can finish or undo it on every one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardSet {
    /// The schema the statement was written for (by the statement rewrite).
    pub template: String,
    /// The physical statement, for the template.
    pub sql: String,
    /// Every shard, in the order the job applies them.
    pub shards: Vec<ShardRef>,
}

impl ShardSet {
    /// The shard schemas, in order.
    ///
    /// # Errors
    /// `Internal` for a name that isn't a schema name.
    pub fn schemas(&self) -> Result<Vec<SchemaName>> {
        self.shards
            .iter()
            .map(|s| {
                s.schema
                    .parse::<SchemaName>()
                    .map_err(|e| SprigError::internal(format!("bad shard in a DDL job: {e}")))
            })
            .collect()
    }

    /// The template schema.
    ///
    /// # Errors
    /// `Internal` for a bad name.
    pub fn template(&self) -> Result<SchemaName> {
        self.template
            .parse::<SchemaName>()
            .map_err(|e| SprigError::internal(format!("bad template in a DDL job: {e}")))
    }

    /// Every shard with its cluster, for executors that route by schema.
    #[must_use]
    pub fn routes(&self) -> Vec<(SchemaName, Option<ClusterId>)> {
        self.shards
            .iter()
            .filter_map(|s| {
                let schema = s.schema.parse::<SchemaName>().ok()?;
                let cluster = s.cluster.as_deref().and_then(|c| ClusterId::new(c).ok());
                Some((schema, cluster))
            })
            .collect()
    }
}

/// The multi-shard DDL jobs among `rows` (a tenant catalog's settings)
/// that change any of `schemas`: the mover's `group-schema` lock. A split, merge or move of those shards waits while this
/// is not empty; a DDL job waits for running mover jobs on its shards.
#[must_use]
pub fn jobs_on_shards(rows: &[crate::store::SettingRow], schemas: &[SchemaName]) -> Vec<JobId> {
    DdlIntent::all(rows)
        .into_iter()
        .filter(|i| {
            i.shards.as_ref().is_some_and(|set| {
                set.shards
                    .iter()
                    .any(|r| schemas.iter().any(|s| s.to_string() == r.schema))
            })
        })
        .map(|i| i.job)
        .collect()
}

/// Which shards an object statement goes to, decided at prepare time.
#[derive(Debug, Clone)]
pub(super) struct Fanout {
    db: DatabaseRow,
    /// Per object: its table (the group decides the shards), or `None` for
    /// database-wide objects.
    tables: Vec<Option<String>>,
    /// The statement touches more than one database.
    mixed: bool,
}

fn table_of(kind: ObjectKind, name: &str) -> Option<String> {
    (kind == ObjectKind::Table).then(|| name.to_owned())
}

impl Fanout {
    /// A statement on one object.
    pub(super) fn of(db: &DatabaseRow, kind: ObjectKind, name: &str) -> Self {
        Self {
            db: db.clone(),
            tables: vec![table_of(kind, name)],
            mixed: false,
        }
    }

    /// `CREATE`: a trigger goes where its table is.
    pub(super) fn of_create(
        db: &DatabaseRow,
        kind: ObjectKind,
        name: &str,
        parent: Option<&str>,
    ) -> Self {
        let table = match (kind, parent) {
            (ObjectKind::Trigger, Some(p)) => Some(p.to_owned()),
            _ => table_of(kind, name),
        };
        Self {
            db: db.clone(),
            tables: vec![table],
            mixed: false,
        }
    }

    /// `DROP` of several objects.
    pub(super) fn of_drop(
        db: &DatabaseRow,
        kind: ObjectKind,
        found: &[(DatabaseRow, ObjectRow)],
    ) -> Self {
        Self {
            db: db.clone(),
            tables: found.iter().map(|(_, o)| table_of(kind, &o.name)).collect(),
            mixed: found.iter().any(|(d, _)| d.db_id != db.db_id),
        }
    }

    /// `RENAME TABLE`: the sources' groups.
    pub(super) fn of_rename(
        db: &DatabaseRow,
        moves: &[(SchemaName, String, SchemaName, String)],
    ) -> Self {
        Self {
            db: db.clone(),
            tables: moves.iter().map(|m| Some(m.1.clone())).collect(),
            mixed: moves.iter().any(|m| m.0 != m.2),
        }
    }
}

fn unsupported(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::DdlUnsupported, msg.into())
}

/// The shards of `fan`, or `None` when the statement stays single-shard
/// (the template alone).
async fn shard_set(
    tenant: TenantId,
    fan: &Fanout,
    physical: &[PhysicalDdl],
    exec: &dyn DdlExecutor,
) -> Result<Option<ShardSet>> {
    let [stmt] = physical else {
        return Ok(None);
    };
    let template = stmt.schema;
    let mut shards: Option<Vec<SchemaName>> = None;
    for t in &fan.tables {
        let target = t
            .as_deref()
            .map_or(ShardTarget::Database, ShardTarget::Table);
        let s = exec.shards(tenant, &fan.db, target).await?;
        match &shards {
            None => shards = Some(s),
            Some(prev) if *prev != s => {
                return Err(unsupported(
                    "the statement names tables in different table groups; run it once per table",
                ));
            }
            Some(_) => {}
        }
    }
    let shards = shards.unwrap_or_default();
    if shards.is_empty() {
        return Err(SprigError::internal(format!(
            "database {} has no physical placement",
            fan.db.name
        )));
    }
    if shards == [template] {
        return Ok(None);
    }
    if fan.mixed {
        return Err(unsupported(
            "a schema change on a sharded table can't also name another database",
        ));
    }
    Ok(Some(ShardSet {
        template: template.to_string(),
        sql: stmt.sql.clone(),
        shards: shards
            .iter()
            .map(|s| ShardRef {
                schema: s.to_string(),
                cluster: exec.cluster_of(s).map(|c| c.to_string()),
            })
            .collect(),
    }))
}

/// The template statement for shard `to`.
pub(super) fn shard_stmt(
    stmt: &PhysicalDdl,
    template: &SchemaName,
    to: &SchemaName,
) -> PhysicalDdl {
    let swap = |s: &SchemaName| if s == template { *to } else { *s };
    let (sql, effect) = match &stmt.effect {
        // Drops are re-run idempotently: the catalog already checked the
        // objects exist.
        Effect::Drop { kind, names } => (
            drop_sql(to, *kind, names),
            Effect::Drop {
                kind: *kind,
                names: names.clone(),
            },
        ),
        Effect::Rename { moves } => (
            for_shard(&stmt.sql, template, to),
            Effect::Rename {
                moves: moves
                    .iter()
                    .map(|(a, b, c, d)| (swap(a), b.clone(), swap(c), d.clone()))
                    .collect(),
            },
        ),
        e => (for_shard(&stmt.sql, template, to), e.clone()),
    };
    PhysicalDdl {
        schema: *to,
        sql,
        effect,
    }
}

/// `TRUNCATE` (no job) on every shard of its table.
pub(super) async fn fan_out(
    tenant: TenantId,
    stmt: &PhysicalDdl,
    fan: Option<&Fanout>,
    exec: &dyn DdlExecutor,
) -> Result<Vec<PhysicalDdl>> {
    let Some(fan) = fan else {
        return Ok(vec![stmt.clone()]);
    };
    match shard_set(tenant, fan, std::slice::from_ref(stmt), exec).await? {
        None => Ok(vec![stmt.clone()]),
        Some(set) => {
            let t = set.template()?;
            Ok(set
                .schemas()?
                .iter()
                .map(|s| shard_stmt(stmt, &t, s))
                .collect())
        }
    }
}

/// The object whose definition a statement changes, for undo.
fn subject(effect: &Effect) -> Option<(ObjectKind, &str)> {
    match effect {
        Effect::Create { kind, name } | Effect::Alter { kind, name } => Some((*kind, name)),
        _ => None,
    }
}

/// Runs one shard's statement; an `ALTER TABLE` with its algorithm named,
/// cheapest first, starting from `hint`. Returns the method used.
pub(super) async fn run_stmt(
    exec: &dyn DdlExecutor,
    st: &PhysicalDdl,
    hint: &mut Option<Method>,
) -> Result<Method> {
    let table_change = matches!(
        st.effect,
        Effect::Alter {
            kind: ObjectKind::Table,
            ..
        }
    );
    let methods = if table_change {
        shards::methods(&st.sql, *hint)
    } else {
        vec![Method::AsWritten]
    };
    let mut last = None;
    for m in methods {
        let sql = shards::with_algorithm(&st.sql, m);
        let r = exec.execute(&PhysicalDdl { sql, ..st.clone() }).await;
        match r {
            Ok(()) => {
                if table_change {
                    *hint = Some(m);
                    if m == Method::AsWritten {
                        tracing::warn!(schema = %st.schema,
                            "no online algorithm for this change; it ran as written and may have blocked writes on the shard");
                    }
                }
                metrics::counter!("sprig_ddl_shard_steps_total", "method" => m.label(), "outcome" => "ok")
                    .increment(1);
                return Ok(m);
            }
            Err(e) if m != Method::AsWritten && errno(&e).is_some_and(shards::method_refused) => {
                last = Some(e);
            }
            Err(e) => {
                metrics::counter!("sprig_ddl_shard_steps_total", "method" => m.label(), "outcome" => "error")
                    .increment(1);
                return Err(e);
            }
        }
    }
    Err(last.unwrap_or_else(|| SprigError::internal("no method to run a schema change")))
}

/// The inverse statement for shard `schema` as a physical statement.
fn inverse(effect: &Effect, schema: SchemaName, undo: Undo) -> Option<PhysicalDdl> {
    let Undo::Sql(sql) = undo else {
        return None;
    };
    let effect = match effect {
        Effect::Create { kind, name } if sql.starts_with("DROP") => Effect::Drop {
            kind: *kind,
            names: vec![name.clone()],
        },
        Effect::Create { kind, name } | Effect::Alter { kind, name }
            if *kind != ObjectKind::Table || sql.starts_with("CREATE") =>
        {
            Effect::Restore {
                kind: *kind,
                name: name.clone(),
            }
        }
        Effect::Rename { moves } => Effect::Rename {
            moves: moves
                .iter()
                .rev()
                .map(|(a, b, c, d)| (*c, d.clone(), *a, b.clone()))
                .collect(),
        },
        e => e.clone(),
    };
    Some(PhysicalDdl {
        schema,
        sql,
        effect,
    })
}

/// What undoing the changed shards came to.
enum Undone {
    /// Every changed shard is back to its old definition.
    Back,
    /// The change removed data: it can only roll forward.
    Forward,
}

/// Whether two `SHOW CREATE` texts, read on shards `a` and `b`, are the
/// same definition.
fn same_def(a: &str, sa: &SchemaName, b: &str, sb: &SchemaName) -> bool {
    comparable(&for_shard(a, sa, sb)) == comparable(b)
}

/// Logs (and counts) shards whose definitions differ after a job.
pub(super) async fn check_skew(
    tenant: TenantId,
    job: JobId,
    set: &ShardSet,
    targets: &[FactTarget],
    exec: &dyn DdlExecutor,
) {
    let Ok(shards) = set.schemas() else { return };
    let Some(first) = shards.first() else { return };
    for t in targets {
        let Ok(Some(base)) = exec.show_create(first, t.kind, &t.name).await else {
            continue;
        };
        for s in &shards[1..] {
            if let Ok(Some(other)) = exec.show_create(s, t.kind, &t.name).await
                && !same_def(&other, s, &base, first)
            {
                metrics::counter!("sprig_ddl_shard_skew_total").increment(1);
                tracing::warn!(%tenant, %job, shard = %s, object = %t.name,
                    "a shard's definition differs from the first shard's after a schema change");
            }
        }
    }
}

impl<B: DdlBackend> DdlEngine<B>
where
    B::Tx: DdlTx,
{
    /// Decides a prepared job's shards and records them in its intent row
    /// (inside phase 1's transaction).
    pub(super) async fn place(
        &self,
        locked: &mut Locked<B>,
        tenant: TenantId,
        mut p: Plan,
        exec: &dyn DdlExecutor,
    ) -> Result<Planned> {
        let Some(fan) = p.fanout.take() else {
            return Ok(Planned::Go(Box::new(p)));
        };
        let Some(set) = shard_set(tenant, &fan, &p.physical, exec).await? else {
            return Ok(Planned::Go(Box::new(p)));
        };
        let first = *set
            .schemas()?
            .first()
            .ok_or_else(|| SprigError::internal("a shard set without shards"))?;
        for t in &mut p.targets {
            t.schema = first;
        }
        p.intent.shards = Some(set);
        let previous = locked.previous();
        let row = p.intent.row(previous, self.clock.wall())?;
        locked.tx().put_internal(&row).await?;
        Ok(Planned::Go(Box::new(p)))
    }

    /// Phase 2 of a multi-shard job (module docs).
    #[allow(clippy::too_many_lines)] // the job's sequence, in order
    pub(super) async fn apply_shards(
        &self,
        tenant: TenantId,
        plan: &Plan,
        set: &ShardSet,
        exec: &dyn DdlExecutor,
    ) -> Result<()> {
        let job = plan.intent.job;
        let shards = set.schemas()?;
        let template = set.template()?;
        let tpl = plan
            .physical
            .first()
            .ok_or_else(|| SprigError::internal("a multi-shard job without a statement"))?;
        let stmts: Vec<PhysicalDdl> = shards
            .iter()
            .map(|s| shard_stmt(tpl, &template, s))
            .collect();
        let mapped = |e: SprigError| map_names(e, tenant, &plan.names);
        // Never beside a split, merge or
        // shard move of these shards. The intent row is written; a shard
        // job admitted from now on waits for it (`jobs_on_shards`).
        if let Err(e) = exec.wait_for_shard_jobs(tenant, &shards).await {
            self.rollback_quietly(tenant, &plan.intent).await;
            return Err(e);
        }
        let replace = matches!(plan.intent.op, IntentOp::Create { replace: true, .. });
        // The old definition (for undo) and, for ALTER TABLE, agreement.
        let mut before: Option<String> = None;
        if let Some((kind, name)) = subject(&tpl.effect)
            && (replace || matches!(tpl.effect, Effect::Alter { .. }))
        {
            let read = exec.show_create(&shards[0], kind, name).await;
            before = match read {
                Ok(b) => b,
                Err(e) => {
                    self.rollback_quietly(tenant, &plan.intent).await;
                    return Err(mapped(e));
                }
            };
            if kind == ObjectKind::Table
                && let Some(b) = &before
            {
                for s in &shards[1..] {
                    let other = match exec.show_create(s, kind, name).await {
                        Ok(o) => o,
                        Err(e) => {
                            self.rollback_quietly(tenant, &plan.intent).await;
                            return Err(mapped(e));
                        }
                    };
                    if !other
                        .as_deref()
                        .is_some_and(|o| same_def(o, s, b, &shards[0]))
                    {
                        self.rollback_quietly(tenant, &plan.intent).await;
                        return Err(SprigError::new(
                            SprigCode::DdlShardDrift,
                            format!(
                                "the shards of table '{name}' disagree on its definition; \
                                 no shard was changed"
                            ),
                        ));
                    }
                }
            }
        }
        let mut hint = None;
        let mut applied: Vec<usize> = Vec::new();
        for (i, st) in stmts.iter().enumerate() {
            self.fail(tenant, DdlStep::Shard(i))?;
            let Err(e) = run_stmt(exec, st, &mut hint).await else {
                applied.push(i);
                continue;
            };
            if outcome_unknown(&e) {
                tracing::warn!(%tenant, %job, shard = %st.schema, error = %e,
                    "multi-shard DDL outcome unknown; the reconciler will finish or undo it");
                return Err(mapped(e));
            }
            if applied.is_empty() {
                self.rollback_quietly(tenant, &plan.intent).await;
                return Err(mapped(e));
            }
            match self
                .undo_applied(tenant, &shards, &stmts, &applied, before.as_deref(), exec)
                .await?
            {
                Undone::Back => {
                    tracing::info!(%tenant, %job, shard = %st.schema, undone = applied.len(),
                        "a shard refused the change; the shards already changed were undone");
                    metrics::counter!("sprig_ddl_jobs_total", "kind" => "schema", "outcome" => "rolled_back")
                        .increment(1);
                    self.rollback_quietly(tenant, &plan.intent).await;
                    return Err(mapped(e));
                }
                Undone::Forward => {
                    let mut last = e;
                    let mut done = false;
                    let mut wait = Duration::from_millis(200);
                    for _ in 0..FORWARD_RETRIES {
                        tokio::time::sleep(wait).await;
                        wait = wait.saturating_mul(2);
                        match run_stmt(exec, st, &mut hint).await {
                            Ok(_) => {
                                done = true;
                                break;
                            }
                            Err(e2) if outcome_unknown(&e2) => return Err(mapped(e2)),
                            Err(e2) => last = e2,
                        }
                    }
                    if !done {
                        tracing::warn!(%tenant, %job, shard = %st.schema, error = %last,
                            "a change that removes data failed on a shard after others ran it; the reconciler will finish it");
                        return Err(mapped(SprigError::new(
                            SprigCode::DdlFailed,
                            format!(
                                "the change ran on {} of {} shards and failed on the next ({}); \
                                 it will be completed in the background",
                                applied.len(),
                                stmts.len(),
                                last.message
                            ),
                        )));
                    }
                    applied.push(i);
                }
            }
        }
        Ok(())
    }

    async fn rollback_quietly(&self, tenant: TenantId, intent: &DdlIntent) {
        if let Err(r) = self.rollback(tenant, intent).await {
            tracing::warn!(%tenant, job = %intent.job, error = %r,
                "rolling back a refused DDL failed; the reconciler will");
        }
    }

    /// Undoes the shards in `applied` (latest first), or says the change
    /// can only roll forward. `before` is the object's old text on
    /// `shards[0]` (`None`: it didn't exist).
    async fn undo_applied(
        &self,
        tenant: TenantId,
        shards: &[SchemaName],
        stmts: &[PhysicalDdl],
        applied: &[usize],
        before: Option<&str>,
        exec: &dyn DdlExecutor,
    ) -> Result<Undone> {
        let effect = &stmts[applied[0]].effect;
        if matches!(effect, Effect::Drop { .. } | Effect::DropSchema) {
            return Ok(Undone::Forward);
        }
        if let Effect::Create { kind, .. } = effect
            && *kind == ObjectKind::Table
            && before.is_some()
        {
            return Ok(Undone::Forward);
        }
        let mut undos = Vec::new();
        for &j in applied.iter().rev() {
            let s = shards[j];
            let now = match subject(&stmts[j].effect) {
                Some((kind, name)) => exec.show_create(&s, kind, name).await?,
                None => None,
            };
            let u = shards::undo_statement(
                &stmts[j].effect,
                &s,
                before.map(|b| (b, &shards[0])),
                now.as_deref(),
            );
            if u == Undo::Forward {
                return Ok(Undone::Forward);
            }
            undos.push((j, inverse(&stmts[j].effect, s, u)));
        }
        let mut hint = None;
        for (j, u) in undos {
            self.fail(tenant, DdlStep::Undo(j))?;
            if let Some(st) = u {
                run_stmt(exec, &st, &mut hint).await?;
            }
        }
        Ok(Undone::Back)
    }

    /// The reconciler's repair of a multi-shard job (module docs).
    #[allow(clippy::too_many_lines)] // one arm per operation
    pub(super) async fn resolve_shards(
        &self,
        tenant: TenantId,
        intent: &DdlIntent,
        set: &ShardSet,
        snap: &Snapshot,
        exec: &dyn DdlExecutor,
    ) -> Result<Resolution> {
        let shards = set.schemas()?;
        let template = set.template()?;
        let Some(first) = shards.first().copied() else {
            return self.undo(tenant, intent).await;
        };
        let object = |id| {
            snap.objects
                .iter()
                .find(|o| o.object_id == id)
                .ok_or_else(|| SprigError::internal(format!("object {id} is not in the catalog")))
        };
        let names = |id| {
            snap.databases
                .iter()
                .find(|d| d.db_id == id && d.state != crate::store::DbState::Dropped)
                .map(|d| d.name.clone())
        };
        let logical = |t: &str| comparable(&logical_text(t, tenant, &names));
        match &intent.op {
            IntentOp::Create { object: id, .. } | IntentOp::Alter { object: id } => {
                let o = object(*id)?;
                let existed = !matches!(intent.op, IntentOp::Create { replace: false, .. });
                let before = if existed {
                    self.latest_create_sql(tenant, *id).await?
                } else {
                    None
                };
                let mut texts = Vec::new();
                for s in &shards {
                    texts.push(exec.show_create(s, o.kind, &o.name).await?);
                }
                let changed: Vec<bool> = texts
                    .iter()
                    .map(|t| match (existed, t, &before) {
                        (false, t, _) => t.is_some(),
                        (true, None, _) => false,
                        (true, Some(t), Some(b)) => logical(t) != comparable(b),
                        (true, Some(_), None) => true,
                    })
                    .collect();
                if existed && before.is_none() {
                    // No history to judge by: publish only if the shards agree.
                    let all_same = texts.iter().zip(&shards).all(|(t, s)| {
                        t.as_deref()
                            .zip(texts[0].as_deref())
                            .is_some_and(|(t, f)| same_def(t, s, f, &first))
                    });
                    if !all_same {
                        return Ok(Resolution::Left {
                            reason: "the shards disagree and the catalog has no definition to \
                                     compare with"
                                .to_owned(),
                        });
                    }
                }
                let n = changed.iter().filter(|c| **c).count();
                let facts = |text: Option<String>| Facts {
                    schema: None,
                    create_sql: std::collections::BTreeMap::from([(*id, text)]),
                };
                if n == shards.len() {
                    return self
                        .done(tenant, intent, &facts(texts[0].clone()), false)
                        .await;
                }
                if n == 0 {
                    return self.undo(tenant, intent).await;
                }
                // Mixed: a shard that didn't change holds the old text.
                let Some(u) = changed.iter().position(|c| !c) else {
                    return self.undo(tenant, intent).await;
                };
                let old = texts[u].clone();
                let effect = if existed {
                    Effect::Alter {
                        kind: o.kind,
                        name: o.name.clone(),
                    }
                } else {
                    Effect::Create {
                        kind: o.kind,
                        name: o.name.clone(),
                    }
                };
                // Can every changed shard be undone?
                let mut undos = Vec::new();
                let mut forward = false;
                for (i, s) in shards.iter().enumerate().filter(|(i, _)| changed[*i]) {
                    let un = shards::undo_statement(
                        &effect,
                        s,
                        old.as_deref().map(|t| (t, &shards[u])),
                        texts[i].as_deref(),
                    );
                    if un == Undo::Forward {
                        forward = true;
                        break;
                    }
                    undos.push((i, inverse(&effect, *s, un)));
                }
                if forward {
                    // Removed data: run the statement on the rest.
                    let tpl = PhysicalDdl {
                        schema: template,
                        sql: set.sql.clone(),
                        effect,
                    };
                    let mut hint = None;
                    for (i, s) in shards.iter().enumerate().filter(|(i, _)| !changed[*i]) {
                        self.fail(tenant, DdlStep::Shard(i))?;
                        run_stmt(exec, &shard_stmt(&tpl, &template, s), &mut hint).await?;
                    }
                    let text = exec.show_create(&first, o.kind, &o.name).await?;
                    return self.done(tenant, intent, &facts(text), true).await;
                }
                let mut hint = None;
                for (i, st) in undos {
                    self.fail(tenant, DdlStep::Undo(i))?;
                    if let Some(st) = st {
                        run_stmt(exec, &st, &mut hint).await?;
                    }
                }
                self.undo(tenant, intent).await
            }
            IntentOp::Drop { objects } => {
                for id in objects {
                    let o = object(*id)?;
                    if o.state == crate::store::ObjectState::Dropped {
                        continue;
                    }
                    for s in &shards {
                        let names = vec![o.name.clone()];
                        exec.execute(&PhysicalDdl {
                            schema: *s,
                            sql: drop_sql(s, o.kind, &names),
                            effect: Effect::Drop {
                                kind: o.kind,
                                names,
                            },
                        })
                        .await?;
                    }
                }
                self.done(tenant, intent, &Facts::default(), true).await
            }
            IntentOp::Rename { moves } => {
                let mut state = Vec::new();
                for s in &shards {
                    let mut src = 0;
                    let mut dst = 0;
                    for m in moves {
                        let o = object(m.object)?;
                        if exec.show_create(s, o.kind, &o.name).await?.is_some() {
                            src += 1;
                        }
                        if exec.show_create(s, o.kind, &m.name).await?.is_some() {
                            dst += 1;
                        }
                    }
                    let k = moves.len();
                    state.push(if dst == k && src == 0 {
                        Some(true)
                    } else if src == k && dst == 0 {
                        Some(false)
                    } else {
                        None
                    });
                }
                if state.iter().any(Option::is_none) {
                    return Ok(Resolution::Left {
                        reason: "a shard is half renamed".to_owned(),
                    });
                }
                if state.iter().all(|s| *s == Some(true)) {
                    let mut create_sql = std::collections::BTreeMap::new();
                    for m in moves {
                        let o = object(m.object)?;
                        create_sql
                            .insert(m.object, exec.show_create(&first, o.kind, &m.name).await?);
                    }
                    let facts = Facts {
                        schema: None,
                        create_sql,
                    };
                    return self.done(tenant, intent, &facts, false).await;
                }
                // Undo the renamed shards.
                let mut back = Vec::new();
                for m in moves.iter().rev() {
                    let o = object(m.object)?;
                    back.push((m.name.clone(), o.name.clone()));
                }
                for (i, s) in shards.iter().enumerate() {
                    if state[i] != Some(true) {
                        continue;
                    }
                    self.fail(tenant, DdlStep::Undo(i))?;
                    let q = quote(&s.to_string());
                    let list: Vec<String> = back
                        .iter()
                        .map(|(a, b)| format!("{q}.{} TO {q}.{}", quote(a), quote(b)))
                        .collect();
                    exec.execute(&PhysicalDdl {
                        schema: *s,
                        sql: format!("RENAME TABLE {}", list.join(", ")),
                        effect: Effect::Rename {
                            moves: back
                                .iter()
                                .map(|(a, b)| (*s, a.clone(), *s, b.clone()))
                                .collect(),
                        },
                    })
                    .await?;
                }
                self.undo(tenant, intent).await
            }
            IntentOp::CreateDatabase { .. } | IntentOp::DropDatabase { .. } => {
                Ok(Resolution::Left {
                    reason: "a database job with a shard set".to_owned(),
                })
            }
        }
    }
}
