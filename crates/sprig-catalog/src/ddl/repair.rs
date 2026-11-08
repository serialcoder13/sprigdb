//! Repairs the reconciler makes through the DDL path (chapter 08,
//! "Reconciler"): finishing or rolling back jobs whose process died, and
//! adopting objects created outside SprigDB.
//!
//! ```text
//! | Finding                                      | Action                                   |
//! | Transient state older than stuck_state_min.  | Finish if the physical state matches;    |
//! |                                              | else roll back (drops roll forward)      |
//! | Table created outside SprigDB                | Adopt it                                 |
//! ```

use std::collections::BTreeMap;

use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::names::{SchemaName, is_internal_object};
use sprig_common::{Result, SprigError};

use super::backend::{DdlBackend, DdlTx};
use super::exec::{DdlExecutor, Effect, PhysicalDdl, PhysicalObject, drop_sql};
use super::{
    DdlEngine, DdlIntent, FactTarget, Facts, IntentOp, gather, logical_text, object_key, sha256,
    shard_schemas, single_schema,
};
use crate::store::{
    Counter, DatabaseRow, DbState, ObjectFlags, ObjectKind, ObjectRow, ObjectState,
    SchemaVersionRow, Snapshot,
};
use crate::version::{ChangeOpts, VersionKind};

/// What [`DdlEngine::resolve`] did with a stuck job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// The statement had taken effect: its result is published.
    Finished {
        /// The change's version.
        version: u64,
    },
    /// The statement hadn't taken effect: phase 1 is undone.
    RolledBack,
    /// A drop was completed (run again, idempotently) and published.
    RolledForward {
        /// The change's version.
        version: u64,
    },
    /// The physical state is ambiguous; left for an operator.
    Left {
        /// Why.
        reason: String,
    },
    /// The job's intent row is gone (it finished meanwhile).
    Gone,
}

impl Resolution {
    /// The metric and decision label.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Resolution::Finished { .. } => "finished",
            Resolution::RolledBack => "rolled_back",
            Resolution::RolledForward { .. } => "rolled_forward",
            Resolution::Left { .. } => "left",
            Resolution::Gone => "gone",
        }
    }
}

struct Rows<'a> {
    dbs: BTreeMap<DbId, &'a DatabaseRow>,
    objects: BTreeMap<ObjectId, &'a ObjectRow>,
}

impl<'a> Rows<'a> {
    fn of(snap: &'a Snapshot) -> Self {
        Self {
            dbs: snap.databases.iter().map(|d| (d.db_id, d)).collect(),
            objects: snap.objects.iter().map(|o| (o.object_id, o)).collect(),
        }
    }

    fn db(&self, id: DbId) -> Result<&'a DatabaseRow> {
        self.dbs
            .get(&id)
            .copied()
            .ok_or_else(|| SprigError::internal(format!("database {id} is not in the catalog")))
    }

    fn object(&self, id: ObjectId) -> Result<&'a ObjectRow> {
        self.objects
            .get(&id)
            .copied()
            .ok_or_else(|| SprigError::internal(format!("object {id} is not in the catalog")))
    }

    fn name_of(&self, id: DbId) -> Option<String> {
        self.dbs
            .get(&id)
            .filter(|d| d.state != DbState::Dropped)
            .map(|d| d.name.clone())
    }
}

impl<B: DdlBackend> DdlEngine<B>
where
    B::Tx: DdlTx,
{
    /// Finishes, rolls back or rolls forward a job whose process died,
    /// from the physical state (module docs). `snap` is a full snapshot
    /// read after the intent was found. The caller checks the intent's
    /// age and that no DDL runs in its schemas.
    ///
    /// # Errors
    /// The catalog's and the cluster's errors; the job is left as it was.
    #[allow(clippy::too_many_lines)] // one arm per operation
    pub async fn resolve(
        &self,
        tenant: TenantId,
        intent: &DdlIntent,
        snap: &Snapshot,
        exec: &dyn DdlExecutor,
    ) -> Result<Resolution> {
        let rows = Rows::of(snap);
        match &intent.op {
            IntentOp::CreateDatabase { db } => {
                let d = rows.db(*db)?;
                let schemas = shard_schemas(tenant, d);
                let Some(first) = schemas.first() else {
                    return self.undo(tenant, intent).await;
                };
                let mut all = true;
                for s in &schemas {
                    all &= exec.schema(s).await?.is_some();
                }
                if all {
                    let facts = gather(exec, Some(first), &[]).await?;
                    self.done(tenant, intent, &facts, false).await
                } else {
                    // Created on no shard (or some): drop what exists, then
                    // forget the row.
                    for s in &schemas {
                        exec.execute(&drop_schema(*s)).await?;
                    }
                    self.undo(tenant, intent).await
                }
            }
            IntentOp::DropDatabase { db } => {
                let d = rows.db(*db)?;
                for s in shard_schemas(tenant, d) {
                    exec.execute(&drop_schema(s)).await?;
                }
                self.done(tenant, intent, &Facts::default(), true).await
            }
            IntentOp::Create {
                object,
                replace: false,
            } => {
                let o = rows.object(*object)?;
                let t = target(tenant, &rows, o, o.db_id, &o.name)?;
                match exec.show_create(&t.schema, t.kind, &t.name).await? {
                    Some(text) => {
                        let facts = Facts {
                            schema: None,
                            create_sql: BTreeMap::from([(t.object, Some(text))]),
                        };
                        self.done(tenant, intent, &facts, false).await
                    }
                    None => self.undo(tenant, intent).await,
                }
            }
            IntentOp::Create {
                object,
                replace: true,
            }
            | IntentOp::Alter { object } => {
                let o = rows.object(*object)?;
                let t = target(tenant, &rows, o, o.db_id, &o.name)?;
                let Some(now) = exec.show_create(&t.schema, t.kind, &t.name).await? else {
                    // Gone physically: undo the marks; the reconciler
                    // reports the missing object.
                    return self.undo(tenant, intent).await;
                };
                let before = self.latest_create_sql(tenant, *object).await?;
                let now_logical = logical_text(&now, tenant, &|id| rows.name_of(id));
                if before.as_deref() == Some(now_logical.as_str()) {
                    self.undo(tenant, intent).await
                } else {
                    let facts = Facts {
                        schema: None,
                        create_sql: BTreeMap::from([(t.object, Some(now))]),
                    };
                    self.done(tenant, intent, &facts, false).await
                }
            }
            IntentOp::Drop { objects } => {
                for id in objects {
                    let o = rows.object(*id)?;
                    if o.state == ObjectState::Dropped {
                        continue;
                    }
                    let d = rows.db(o.db_id)?;
                    let schema = single_schema(tenant, d)?;
                    let names = vec![o.name.clone()];
                    exec.execute(&PhysicalDdl {
                        schema,
                        sql: drop_sql(&schema, o.kind, &names),
                        effect: Effect::Drop {
                            kind: o.kind,
                            names,
                        },
                    })
                    .await?;
                }
                self.done(tenant, intent, &Facts::default(), true).await
            }
            IntentOp::Rename { moves } => {
                let mut src_present = 0;
                let mut dst_present = 0;
                let mut targets = Vec::new();
                let sources: Vec<(DbId, String)> = moves
                    .iter()
                    .filter_map(|m| {
                        rows.objects
                            .get(&m.object)
                            .map(|o| (o.db_id, o.name_key.clone()))
                    })
                    .collect();
                for m in moves {
                    let o = rows.object(m.object)?;
                    // A swap's names are both sources and targets: the
                    // physical names can't tell which side won.
                    if sources.iter().any(|(d, k)| *d == m.db && *k == m.name_key) {
                        return Ok(Resolution::Left {
                            reason:
                                "a rename that swaps names can't be judged from the physical state"
                                    .to_owned(),
                        });
                    }
                    let src = target(tenant, &rows, o, o.db_id, &o.name)?;
                    let dst = target(tenant, &rows, o, m.db, &m.name)?;
                    if exec
                        .show_create(&src.schema, src.kind, &src.name)
                        .await?
                        .is_some()
                    {
                        src_present += 1;
                    }
                    if exec
                        .show_create(&dst.schema, dst.kind, &dst.name)
                        .await?
                        .is_some()
                    {
                        dst_present += 1;
                    }
                    targets.push(dst);
                }
                let n = moves.len();
                if dst_present == n && src_present == 0 {
                    let facts = gather(exec, None, &targets).await?;
                    self.done(tenant, intent, &facts, false).await
                } else if src_present == n && dst_present == 0 {
                    self.undo(tenant, intent).await
                } else {
                    Ok(Resolution::Left {
                        reason: format!(
                            "rename half applied: {src_present} of {n} sources and \
                             {dst_present} of {n} targets exist"
                        ),
                    })
                }
            }
        }
    }

    async fn done(
        &self,
        tenant: TenantId,
        intent: &DdlIntent,
        facts: &Facts,
        forward: bool,
    ) -> Result<Resolution> {
        Ok(match self.finish(tenant, intent, facts).await? {
            Some(version) if forward => Resolution::RolledForward { version },
            Some(version) => Resolution::Finished { version },
            None => Resolution::Gone,
        })
    }

    async fn undo(&self, tenant: TenantId, intent: &DdlIntent) -> Result<Resolution> {
        Ok(if self.rollback(tenant, intent).await? {
            Resolution::RolledBack
        } else {
            Resolution::Gone
        })
    }

    async fn latest_create_sql(
        &self,
        tenant: TenantId,
        object: ObjectId,
    ) -> Result<Option<String>> {
        let mut locked = self.versioner().lock(tenant, VersionKind::Catalog).await?;
        let r = locked.tx().latest_schema_version(object).await;
        locked.release().await?;
        Ok(r?.and_then(|r| r.create_sql))
    }

    /// Adopts objects found in `db`'s schema with no catalog row (chapter
    /// 08: "Table created outside SprigDB: adopt it"): one versioned
    /// change inserting them `active` at schema version 1, with
    /// `SHOW CREATE` as their history. Internal (`__sprig_`) objects and
    /// names that gained a row meanwhile are skipped. Returns the version,
    /// or `None` if nothing was adopted.
    ///
    /// # Errors
    /// The catalog's and the cluster's errors.
    pub async fn adopt(
        &self,
        tenant: TenantId,
        db: &DatabaseRow,
        found: &[PhysicalObject],
        exec: &dyn DdlExecutor,
        lower_case_table_names: bool,
    ) -> Result<Option<(u64, usize)>> {
        let schema = single_schema(tenant, db)?;
        let mut texts = Vec::new();
        for p in found.iter().filter(|p| !is_internal_object(&p.name)) {
            if let Some(t) = exec.show_create(&schema, p.kind, &p.name).await? {
                texts.push((p, t));
            }
        }
        if texts.is_empty() {
            return Ok(None);
        }
        let mut locked = self.versioner().lock(tenant, VersionKind::Catalog).await?;
        let tx = locked.tx();
        let mut names: BTreeMap<DbId, String> = BTreeMap::new();
        for d in tx.databases().await? {
            if d.state != DbState::Dropped {
                names.insert(d.db_id, d.name);
            }
        }
        // Parents first, so triggers find their tables.
        texts.sort_by_key(|(p, _)| p.kind == ObjectKind::Trigger);
        let mut fresh = Vec::new();
        for (p, text) in &texts {
            let key = object_key(p.kind, &p.name, lower_case_table_names);
            if tx
                .object_by_name(db.db_id, p.kind.namespace(), &key)
                .await?
                .is_some()
            {
                continue;
            }
            fresh.push((*p, text.clone(), key));
        }
        if fresh.is_empty() {
            locked.release().await?;
            return Ok(None);
        }
        let mut c = locked.allocate(ChangeOpts::default()).await?;
        let v = c.version();
        let now = self.clock.wall();
        let r: Result<usize> = async {
            let tx = c.tx();
            let mut n = 0;
            for (p, text, key) in &fresh {
                let parent_id = match &p.parent {
                    Some(t) => tx
                        .object_by_name(
                            db.db_id,
                            ObjectKind::Table.namespace(),
                            &object_key(ObjectKind::Table, t, lower_case_table_names),
                        )
                        .await?
                        .map(|o| o.object_id),
                    None => None,
                };
                let id = ObjectId(tx.next_id(Counter::Object).await?);
                tx.insert_object(&ObjectRow {
                    object_id: id,
                    db_id: db.db_id,
                    namespace: p.kind.namespace(),
                    name: p.name.clone(),
                    name_key: key.clone(),
                    kind: p.kind,
                    parent_id,
                    state: ObjectState::Active,
                    schema_version: 1,
                    flags: ObjectFlags::empty(),
                    pending_job_id: None,
                    created_at: now,
                    changed_version: v,
                })
                .await?;
                let create_sql = logical_text(text, tenant, &|id| names.get(&id).cloned());
                tx.insert_schema_version(&SchemaVersionRow {
                    object_id: id,
                    version: 1,
                    ddl: format!("-- adopted by the reconciler\n{create_sql}"),
                    create_sql_hash: Some(sha256(&create_sql)),
                    create_sql: Some(create_sql),
                    ddl_job_id: None,
                    applied_by: None,
                    applied_at: now,
                    changed_version: v,
                })
                .await?;
                n += 1;
            }
            Ok(n)
        }
        .await;
        match r {
            Ok(n) => {
                c.commit().await?;
                Ok(Some((v, n)))
            }
            Err(e) => {
                let _ = c.rollback().await;
                Err(e)
            }
        }
    }
}

fn target(
    tenant: TenantId,
    rows: &Rows<'_>,
    o: &ObjectRow,
    db: DbId,
    name: &str,
) -> Result<FactTarget> {
    let d = rows.db(db)?;
    Ok(FactTarget {
        object: o.object_id,
        schema: single_schema(tenant, d)?,
        kind: o.kind,
        name: name.to_owned(),
    })
}

/// `DROP DATABASE IF EXISTS` of a physical schema.
#[must_use]
pub fn drop_schema(schema: SchemaName) -> PhysicalDdl {
    PhysicalDdl {
        schema,
        sql: format!(
            "DROP DATABASE IF EXISTS {}",
            sprig_common::names::quote(&schema.to_string())
        ),
        effect: Effect::DropSchema,
    }
}
