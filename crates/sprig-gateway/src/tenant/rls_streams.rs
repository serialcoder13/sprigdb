//! Row-level security for paths that stream a table's rows out of the
//! gateway's session rewrite (chapter 29, "Change feed consumers"; RLS
//! round 4, decisions RL36 onward).
//!
//! ```text
//! change topic t.changes read by a principal (SQL, Kafka, a stream trigger,
//! a sink FROM STREAM t.changes):
//!   SELECT on the source table t itself (not only on the name "t.changes")
//!   policies on t bind the principal → 39366 (the topic's records are the
//!     table's rows; they aren't filtered per record)
//! ```
//!
//! The checks are the same at creation (a trigger's or sink's) and at each
//! run, so a policy, role or grant change after creation stops delivery.

use sprig_auth::authz::{Action, SessionGrants, Target};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_streams::model::{StreamDef, StreamKind};

/// Whether `grants` may read change topic `def`'s records: `Ok` for an
/// event stream, or a change topic whose source table the grants may
/// `SELECT` and whose policies don't bind them. 1142 without `SELECT` on
/// the table, 39366 when its policies bind.
///
/// # Errors
/// As above.
pub fn change_topic_readable(
    grants: &dyn SessionGrants,
    cat: &TenantCatalog,
    def: &StreamDef,
    lctn: bool,
    user: &str,
) -> Result<()> {
    if def.kind != StreamKind::Changes {
        return Ok(());
    }
    if grants.unrestricted() {
        return Ok(());
    }
    let table = def.source_name.clone().unwrap_or_default();
    let (Some(object), Some(db)) = (def.source_object, cat.databases.get(def.db)) else {
        // A change topic without its table: nothing to read it by.
        return Err(denied(user, "?", &table));
    };
    let db_key = database_key(&db.name, lctn);
    let on = Target::Object {
        db_key: &db_key,
        db: def.db,
        object,
    };
    if !grants.allows(Action::Select, on) {
        return Err(denied(user, &db.name, &table));
    }
    if grants.row_filter(on, Action::Select).is_some() {
        return Err(bound(&table, &def.name));
    }
    Ok(())
}

/// 39366 for change topic `topic` of table `table`.
#[must_use]
pub fn bound(table: &str, topic: &str) -> SprigError {
    SprigError::new(
        SprigCode::RlsUnsupported,
        format!(
            "ER_SPRIG_RLS_UNSUPPORTED: row-level security on '{table}' binds this user, and \
             change topic '{topic}' carries the table's rows unfiltered"
        ),
    )
}

fn denied(user: &str, db: &str, table: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!("SELECT command denied to user '{user}' for table `{db}`.`{table}`"),
    )
}
