//! A dropped database's logic (chapter 41, "Dropping a database";
//! bugfix-34): `DROP DATABASE` takes every logic module of the database
//! with it, as chapter 08 says it takes every object.
//!
//! ```text
//! each live module of the database (state <> 'dropped'):
//!   logic_modules.state = 'dropped' (its name gets #<id>), versions retired
//!   trigger on a stream or queue  → its index entry logic/msg/<t>/<b> (and a
//!                                   fan-in trigger's member list) removed;
//!                                   its job ends at its next check
//!   after-commit trigger          → its feed subscription logic:b<b> dropped;
//!                                   its job sees the module dropped and ends
//!   schedule                      → its index entry logic/schedules/<t>/<m>
//!                                   removed
//! ```
//!
//! Routines, `CHECK` constraints and MariaDB triggers lived in the
//! database's shard schemas and went with them. Dead letters stay, as
//! they do for a sink's `DROP SINK` without `WITH DEAD LETTERS`.

use std::sync::Arc;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_feed::subs::Subscriptions;
use sprig_meta::MetaStore;

use crate::catalog::{BindingRow, LogicStore, ModuleRow};

/// What a module's binding needs besides its catalog row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cleanup {
    /// A trigger on a stream or queue: its index entry.
    MessageIndex,
    /// An after-commit trigger: its feed subscription.
    Subscription,
    /// A schedule: its index entry.
    ScheduleIndex,
    /// Nothing outside the catalog (functions, procedures, validations,
    /// computed fields: their objects were in the shard schemas).
    None,
}

/// The cleanup a module of `kind` with `binding` needs.
#[must_use]
pub fn cleanup_of(kind: &str, binding: Option<&BindingRow>) -> Cleanup {
    if kind == "schedule" {
        return Cleanup::ScheduleIndex;
    }
    match binding.map(|b| b.timing.as_str()) {
        Some("on_message") => Cleanup::MessageIndex,
        Some("after_commit") => Cleanup::Subscription,
        _ => Cleanup::None,
    }
}

/// Drops every live logic module of database `db_id` of `tenant` (see
/// the module docs) and returns them. A catalog without the logic tables
/// has none.
///
/// # Errors
/// The catalog's or the DC tier's; modules handled before the error stay
/// dropped (a retry finds only the rest).
pub async fn database_dropped(
    c: &mut Conn,
    schema: &str,
    dc: &Arc<dyn MetaStore>,
    tenant: TenantId,
    db_id: u32,
    catalog_version: u64,
) -> Result<Vec<ModuleRow>> {
    let migrated: Option<u8> = c
        .exec_first(
            "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
             AND TABLE_NAME = 'logic_modules'",
            (schema,),
        )
        .await
        .map_err(|e| {
            SprigError::new(
                SprigCode::CatalogUnavailable,
                "the logic catalog is unreachable",
            )
            .with_source(e)
        })?;
    if migrated.is_none() {
        return Ok(Vec::new());
    }
    let store = LogicStore::new(schema);
    let modules = store.modules(c, Some(db_id)).await?;
    for m in &modules {
        let binding = store.binding(c, m.module_id).await?;
        store.mark_dropped(c, m.module_id, catalog_version).await?;
        match cleanup_of(&m.kind, binding.as_ref()) {
            Cleanup::MessageIndex => {
                let Some(b) = &binding else { continue };
                if let Some(def) = crate::messages::source(c, schema, b.binding_id)
                    .await?
                    .and_then(|s| s.fanin_definition())
                {
                    crate::messages::delete_fanin(dc.as_ref(), tenant, b.binding_id, def).await?;
                }
                crate::messages::delete_index(dc.as_ref(), tenant, b.binding_id).await?;
            }
            Cleanup::Subscription => {
                let Some(b) = &binding else { continue };
                Subscriptions::new(Arc::clone(dc))
                    .drop_subscription(tenant, &crate::triggers::subscription_name(b.binding_id))
                    .await?;
            }
            Cleanup::ScheduleIndex => {
                let key = crate::schedule::index_key(dc.as_ref(), tenant, m.module_id)?;
                dc.txn(sprig_meta::Txn {
                    when: Vec::new(),
                    then: vec![sprig_meta::Op::Delete(key)],
                    otherwise: Vec::new(),
                })
                .await?;
            }
            Cleanup::None => {}
        }
        tracing::info!(%tenant, db = db_id, module = m.module_id, name = %m.name, kind = %m.kind,
            "logic module dropped with its database");
    }
    Ok(modules)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(timing: &str) -> BindingRow {
        BindingRow {
            binding_id: 7,
            module_id: 9,
            object_id: Some(11),
            column_name: None,
            timing: timing.to_owned(),
            events: "insert".to_owned(),
            when_fields: Vec::new(),
            enforcement: Some("trigger".to_owned()),
            state: "active".to_owned(),
        }
    }

    #[test]
    fn each_kind_of_module_gets_its_cleanup() {
        assert_eq!(
            cleanup_of("trigger", Some(&binding("on_message"))),
            Cleanup::MessageIndex
        );
        assert_eq!(
            cleanup_of("trigger", Some(&binding("after_commit"))),
            Cleanup::Subscription
        );
        assert_eq!(cleanup_of("schedule", None), Cleanup::ScheduleIndex);
        assert_eq!(
            cleanup_of("validation", Some(&binding("before_write"))),
            Cleanup::None
        );
        assert_eq!(cleanup_of("function", None), Cleanup::None);
    }
}
