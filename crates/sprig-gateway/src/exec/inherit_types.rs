//! Typed tables in inheritance groups (chapter 42 with chapter 32;
//! `inheritance-plan.md` step 6b): a child tenant that receives the tables
//! of a type tree also receives the types, so its type layer plans the
//! same polymorphic reads and writes over them.
//!
//! After [`super::inherit_tree::adopt`] has brought a child's tables up to
//! its parent's, [`adopt_types`] compares the parent's types of those
//! tables with the child's and writes what differs into the child's
//! `types` module: the same names, paths, storage, discriminator and
//! values, under the child's object ids. A child's own subtypes (Kiran's
//! Q2: "a child tenant can declare its own subtypes") and the columns it
//! added to an inherited type stay.

use std::sync::Arc;

use sprig_catalog::ddl::database_key;
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, TenantCatalog};
use sprig_common::ids::ObjectId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_types::model::{FieldDef, TypeDef, TypeState};
use sprig_types::{TypesChange, TypesState, TypesStore};

use crate::tenant::TenantServices;

/// The fields of `want` with the child's own additions to the type kept.
fn merged_fields(want: &[FieldDef], have: Option<&TypeDef>) -> Vec<FieldDef> {
    let mut out = want.to_vec();
    if let Some(h) = have {
        for f in &h.own_fields {
            if !out.iter().any(|w| w.name.eq_ignore_ascii_case(&f.name)) {
                let mut f = f.clone();
                f.ordinal = u16::try_from(out.len() + 1).unwrap_or(u16::MAX);
                out.push(f);
            }
        }
    }
    out
}

fn same_fields(a: &[FieldDef], b: &[FieldDef]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.name.eq_ignore_ascii_case(&y.name)
                && x.data_type == y.data_type
                && x.nullable == y.nullable
                && x.default_expr == y.default_expr
        })
}

fn same_type(a: &TypeDef, b: &TypeDef) -> bool {
    a.parent == b.parent
        && a.root == b.root
        && a.depth == b.depth
        && a.path == b.path
        && a.is_abstract == b.is_abstract
        && a.storage == b.storage
        && a.key_fields == b.key_fields
        && a.discriminator == b.discriminator
        && a.disc_value == b.disc_value
        && a.disc_operand == b.disc_operand
        && same_fields(&a.own_fields, &b.own_fields)
}

/// A tree whose discriminator the parent added or dropped (decision
/// T19): the child's root table is converted before its types follow.
pub(crate) struct Follow {
    /// The logical database, and its id in the child.
    pub(crate) db_name: String,
    pub(crate) child_db: sprig_common::ids::DbId,
    /// The parent's types of the database, and its root.
    pub(crate) parent: Arc<sprig_types::DbTypes>,
    pub(crate) parent_root: Arc<TypeDef>,
    /// The child's types of the database, and its root.
    pub(crate) child: Arc<sprig_types::DbTypes>,
    pub(crate) child_root: Arc<TypeDef>,
}

/// What `child` must write so its types of the tables it inherits from
/// `parent` match the parent's. A tree whose discriminator differs keeps
/// the child's discriminator here: it changes only with the child's table
/// ([`Follow`]).
#[must_use]
pub(crate) fn wanted_types(
    parent: &TenantCatalog,
    child: &TenantCatalog,
    lctn: bool,
) -> (TypesChange, Vec<Follow>) {
    let mut change = TypesChange::default();
    let mut follow = Vec::new();
    let Some(pt) = parent.ext.get::<TypesState>(sprig_types::store::MODULE) else {
        return (change, follow);
    };
    let Some(cs) = sprig_inherit::plan::state_of(child) else {
        return (change, follow);
    };
    let ct = child.ext.get::<TypesState>(sprig_types::store::MODULE);
    for (pdb, pd) in pt.dbs() {
        let Some(db_name) = parent.databases.get(pdb).map(|d| d.name.clone()) else {
            continue;
        };
        let Some(cdb) = child.database(&database_key(&db_name, lctn)) else {
            continue;
        };
        let child_id = |name: &str| -> Option<ObjectId> {
            cs.object_named_ci(cdb.db_id, name)
                .filter(|o| o.type_root.is_some())
                .map(|o| o.object_id)
        };
        let have_db = ct.and_then(|c| c.db(cdb.db_id));
        for t in pd.iter() {
            let Some(id) = child_id(&t.name) else {
                continue;
            };
            let map = |p: ObjectId| pd.by_id(p).and_then(|x| child_id(&x.name));
            let Some(root) = map(t.root) else {
                continue;
            };
            let parent_id = match t.parent {
                Some(p) => match map(p) {
                    Some(x) => Some(x),
                    None => continue,
                },
                None => None,
            };
            let have = have_db.and_then(|d| d.by_id(id)).map(AsRef::as_ref);
            // The roots' discriminators differ: the child's table converts
            // first (once per tree), and its types keep theirs meanwhile.
            let parent_root = pd.by_id(t.root);
            let child_root = have_db.and_then(|d| d.by_id(root));
            let differs = match (parent_root, child_root) {
                (Some(p), Some(c)) => p.discriminator != c.discriminator,
                _ => false,
            };
            if differs
                && t.is_root()
                && let (Some(p), Some(c), Some(cd)) = (parent_root, child_root, have_db)
            {
                follow.push(Follow {
                    db_name: db_name.clone(),
                    child_db: cdb.db_id,
                    parent: Arc::clone(pd),
                    parent_root: Arc::clone(p),
                    child: Arc::clone(cd),
                    child_root: Arc::clone(c),
                });
            }
            let mut want = TypeDef {
                id,
                db: cdb.db_id,
                name: have.map_or_else(|| t.name.clone(), |h| h.name.clone()),
                parent: parent_id,
                root,
                depth: t.depth,
                path: t.path.clone(),
                is_abstract: t.is_abstract,
                storage: t.storage,
                key_fields: t.key_fields.clone(),
                own_fields: merged_fields(&t.own_fields, have),
                discriminator: t.discriminator.clone(),
                disc_value: t.disc_value.clone(),
                disc_operand: t.disc_operand.clone(),
                state: TypeState::Active,
                version: have.map_or(1, |h| h.version),
            };
            if differs {
                want.discriminator = have.and_then(|h| h.discriminator.clone());
                want.disc_value = have.and_then(|h| h.disc_value.clone());
                want.disc_operand = have.and_then(|h| h.disc_operand.clone());
            }
            if have.is_some_and(|h| same_type(h, &want)) {
                continue;
            }
            want.state = TypeState::Active;
            change.fields.push((id, want.own_fields.clone()));
            change.upsert_types.push(want);
        }
    }
    (change, follow)
}

/// Rows of a child's layer table that count for a conversion: not hide
/// markers.
const LIVE_ROWS: &str = "`_sprig_state` <> 'hide'";

/// Converts the child's root tables of `follow` (decision T19) and adds
/// the types they then have to `change`. A tree that can't convert (an own
/// subtype whose rows don't tell its value, rows that disagree) is left as
/// it was, with a warning, until its next catalog change.
async fn follow_discriminators(
    svc: &Arc<TenantServices>,
    child: &TenantCatalog,
    follow: Vec<Follow>,
    lctn: bool,
    change: &mut TypesChange,
    objects: &mut Vec<sprig_inherit::model::InheritableObject>,
) -> Result<()> {
    if follow.is_empty() {
        return Ok(());
    }
    let t = child.tenant;
    let cluster = svc.locator.cluster(t).await?;
    let pool = svc.locator.ctl_pool(&cluster).await?;
    let conn = pool.get_conn().await.map_err(|e| {
        SprigError::new(SprigCode::BackendUnavailable, e.to_string()).with_source(e)
    })?;
    let mut run = ProvRunner { conn };
    for f in follow {
        let schema = sprig_inherit::plan::physical_schema(child, &f.db_name, lctn)?;
        match sprig_types::ddl::follow_discriminator(
            &schema,
            &f.parent,
            &f.parent_root,
            &f.child,
            &f.child_root,
            LIVE_ROWS,
            &mut run,
        )
        .await
        {
            Ok(types) => {
                // The root's type columns stop (or start) being fields:
                // merged views compute them under a discriminator.
                let disc = types.first().is_some_and(|r| r.discriminator.is_some());
                if let Some(o) = sprig_inherit::plan::state_of(child)
                    .and_then(|st| st.object_named_ci(f.child_db, &f.child_root.name))
                {
                    let is_type = |c: &String| {
                        c.eq_ignore_ascii_case("_type") || c.eq_ignore_ascii_case("_type_path")
                    };
                    let mut cols: Vec<String> =
                        o.columns.iter().filter(|c| !is_type(c)).cloned().collect();
                    if !disc {
                        cols.push("_type".to_owned());
                        cols.push("_type_path".to_owned());
                    }
                    if let Some(n) = o.with_slots_for(&cols) {
                        objects.push(n);
                    }
                }
                for ty in types {
                    change.upsert_types.retain(|x| x.id != ty.id);
                    if !change.fields.iter().any(|(id, _)| *id == ty.id) {
                        change.fields.push((ty.id, ty.own_fields.clone()));
                    }
                    change.upsert_types.push(ty);
                }
                metrics::counter!("sprig_inherit_discriminators_followed_total").increment(1);
                tracing::info!(tenant = %t, root = %f.child_root.name, "discriminator followed");
            }
            Err(e) => {
                metrics::counter!("sprig_inherit_discriminators_unfollowed_total").increment(1);
                tracing::warn!(tenant = %t, root = %f.child_root.name, error = %e.message,
                    "the parent's discriminator change can't be followed yet");
            }
        }
    }
    Ok(())
}

/// The type layer's runner on a provisioning connection, physical names
/// ([`follow_discriminators`]).
struct ProvRunner {
    conn: mysql_async::Conn,
}

fn prov_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            SprigError::backend(s.code, state, format!("{} ({})", s.message, s.code))
        }
        other => SprigError::new(SprigCode::BackendUnavailable, other.to_string()),
    }
    .with_source(e)
}

#[async_trait::async_trait]
impl sprig_types::Runner for ProvRunner {
    async fn query(&mut self, sql: &str) -> Result<sprig_types::Rows> {
        use mysql_async::prelude::Queryable;
        let rows: Vec<mysql_async::Row> = self.conn.query(sql).await.map_err(prov_err)?;
        Ok(sprig_types::Rows {
            columns: Vec::new(),
            rows: rows
                .into_iter()
                .map(|r| {
                    r.unwrap()
                        .into_iter()
                        .map(|v| match v {
                            mysql_async::Value::NULL => None,
                            mysql_async::Value::Bytes(b) => Some(b),
                            other => Some(other.as_sql(true).into_bytes()),
                        })
                        .collect()
                })
                .collect(),
        })
    }

    async fn exec(&mut self, sql: &str) -> Result<sprig_types::Done> {
        use mysql_async::prelude::Queryable;
        self.conn.query_drop(sql).await.map_err(prov_err)?;
        Ok(sprig_types::Done {
            affected_rows: self.conn.affected_rows(),
            ..sprig_types::Done::default()
        })
    }

    fn in_transaction(&self) -> bool {
        false
    }

    fn found_rows(&self) -> bool {
        false
    }

    async fn object_id(
        &mut self,
        _db: Option<&str>,
        _table: &str,
    ) -> Result<Option<(sprig_common::ids::DbId, ObjectId)>> {
        Err(SprigError::internal("no catalog in this runner"))
    }

    async fn commit_types(&mut self, _change: &TypesChange) -> Result<()> {
        Err(SprigError::internal("no catalog in this runner"))
    }
}

/// Writes [`wanted_types`] into `child`'s catalog when anything differs:
/// the new catalog version.
///
/// # Errors
/// The catalog's.
pub(crate) async fn adopt_types(
    svc: &Arc<TenantServices>,
    parent: &TenantCatalog,
    child: &TenantCatalog,
    lctn: bool,
) -> Result<Option<u64>> {
    let (mut change, follow) = wanted_types(parent, child, lctn);
    let mut objects = Vec::new();
    Box::pin(follow_discriminators(
        svc,
        child,
        follow,
        lctn,
        &mut change,
        &mut objects,
    ))
    .await?;
    if change.is_empty() {
        return Ok(None);
    }
    let t = child.tenant;
    let ep = svc.locator.writer(t).await?;
    {
        let mut conn = ep.pool.get_conn().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        sprig_types::store::migrate(&mut conn, ep.store.schema()).await?;
    }
    let mut c = svc.catalog.change(t, VersionKind::Catalog).await?;
    let version = c.version();
    let applied = {
        let (store, tx) = c.tx().parts();
        match TypesStore::new(store.schema()).apply(tx, &change).await {
            Ok(()) if !objects.is_empty() => {
                let inh = sprig_inherit::InheritChange {
                    upsert_objects: objects,
                    ..sprig_inherit::InheritChange::default()
                };
                sprig_inherit::InheritStore::new(store.schema())
                    .apply(tx, &inh, version)
                    .await
            }
            other => other,
        }
    };
    if let Err(e) = applied {
        let _ = c.rollback().await;
        return Err(e);
    }
    let v = c.commit().await?;
    metrics::counter!("sprig_inherit_adopted_types_total")
        .increment(change.upsert_types.len() as u64);
    tracing::info!(tenant = %t, types = change.upsert_types.len(), "inherited types adopted");
    Ok(Some(v))
}
