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

/// What `child` must write so its types of the tables it inherits from
/// `parent` match the parent's.
#[must_use]
pub(crate) fn wanted_types(
    parent: &TenantCatalog,
    child: &TenantCatalog,
    lctn: bool,
) -> TypesChange {
    let mut change = TypesChange::default();
    let Some(pt) = parent.ext.get::<TypesState>(sprig_types::store::MODULE) else {
        return change;
    };
    let Some(cs) = sprig_inherit::plan::state_of(child) else {
        return change;
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
            if have.is_some_and(|h| same_type(h, &want)) {
                continue;
            }
            want.state = TypeState::Active;
            change.fields.push((id, want.own_fields.clone()));
            change.upsert_types.push(want);
        }
    }
    change
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
    let change = wanted_types(parent, child, lctn);
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
    let applied = {
        let (store, tx) = c.tx().parts();
        TypesStore::new(store.schema()).apply(tx, &change).await
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
