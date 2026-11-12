//! A dropped database's sinks and connections (chapter 51, "Dropping a
//! database"; bugfix-34): `DROP DATABASE` takes them with it, as chapter
//! 08 says it takes every object of the database.
//!
//! The gateway removes each sink's DC index entry and applies each
//! [`Change`] in a versioned catalog change, as `DROP SINK` and `DROP
//! CONNECTION` do: the sink's tasks end at their next check of the
//! catalog. A connection of the database that a sink of another database
//! still uses stays (`DROP CONNECTION` would refuse it with 39215).

use crate::model::{ConnectionDef, SinkDef};
use crate::sql::Change;
use crate::store::SinksState;

/// What dropping database `db` removes from `state`: its sinks, then its
/// connections that no sink of another database uses.
#[must_use]
pub fn database_plan(state: &SinksState, db: u32) -> (Vec<SinkDef>, Vec<ConnectionDef>) {
    let sinks: Vec<SinkDef> = state
        .sinks
        .values()
        .filter(|s| s.db_id == db)
        .map(|s| (**s).clone())
        .collect();
    let connections = state
        .connections
        .values()
        .filter(|c| c.db_id == db)
        .filter(|c| state.users_of(c.id).iter().all(|s| s.db_id == db))
        .map(|c| (**c).clone())
        .collect();
    (sinks, connections)
}

/// The catalog changes of [`database_plan`], in order.
#[must_use]
pub fn database_changes(sinks: Vec<SinkDef>, connections: Vec<ConnectionDef>) -> Vec<Change> {
    sinks
        .into_iter()
        .map(Change::DropSink)
        .chain(connections.into_iter().map(Change::DropConnection))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::Map;

    use super::*;
    use crate::model::{ConnKind, ConnRef, ConnectionId, SinkId, TargetSpec};

    fn conn(id: u64, db: u32, name: &str) -> Arc<ConnectionDef> {
        Arc::new(ConnectionDef {
            id: ConnectionId(id),
            db_id: db,
            name: name.to_owned(),
            kind: ConnKind::Webhook,
            options: Map::new(),
            hosts: Vec::new(),
            residency_tags: None,
            owner_user_id: 1,
            secrets: Vec::new(),
        })
    }

    fn sink(id: u64, db: u32, name: &str, conn: &ConnectionDef) -> Arc<SinkDef> {
        let mut s = crate::probe::stand_in(db, name, TargetSpec::Webhook { path: None });
        s.id = SinkId(id);
        s.connections = vec![ConnRef::Tenant {
            id: conn.id,
            name: conn.name.clone(),
        }];
        Arc::new(s)
    }

    #[test]
    fn a_dropped_databases_sinks_and_unshared_connections_go() {
        let hook = conn(1, 5, "hook");
        let shared = conn(2, 5, "shared");
        let other = conn(3, 6, "other");
        let mut st = SinksState {
            migrated: true,
            ..SinksState::default()
        };
        for c in [&hook, &shared, &other] {
            st.connections
                .insert((c.db_id, c.name.clone()), Arc::clone(c));
        }
        for s in [
            sink(10, 5, "s1", &hook),
            sink(11, 5, "s2", &shared),
            sink(12, 6, "keeps", &shared),
            sink(13, 6, "own", &other),
        ] {
            st.sinks.insert((s.db_id, s.name.clone()), s);
        }
        let (sinks, conns) = database_plan(&st, 5);
        let names: Vec<&str> = sinks.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["s1", "s2"]);
        // `shared` is used by a sink of database 6: it stays.
        let conns: Vec<&str> = conns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(conns, ["hook"]);
        let changes = database_changes(sinks, Vec::new());
        assert!(matches!(
            changes.as_slice(),
            [Change::DropSink(_), Change::DropSink(_)]
        ));
        assert_eq!(database_plan(&st, 7), (Vec::new(), Vec::new()));
    }
}
