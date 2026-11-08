//! `index_usage` rows (decision O1): the user and migrated indexes whose
//! removal automatic indexing manages.
//!
//! ```text
//! serving ──judged: remove, retire_user_indexes = on──────→ retiring (IGNORED)
//!    │  ↑  └─judged: remove, retire_user_indexes = recommend → recommended
//!    │  │                                                       │ judged: keep
//!    │  └──────────────────────────────────────────────────────┘
//!    │  ↑ a served query regressed while IGNORED, or the setting went off
//! retiring ──usage_sample_reads reads, no regression──→ dropped (definition kept)
//! any ──sprigctl index restore──→ pinned (visible; never judged again)
//! pinned ──sprigctl index unpin──→ serving
//! ```
//!
//! A row appears the first time its index is judged and goes when the user
//! drops the index themselves; a `dropped` row keeps the exact definition
//! from `SHOW CREATE TABLE` so `sprigctl index restore` can recreate it.

use std::time::SystemTime;

use mysql_async::Row;
use mysql_async::prelude::Queryable;
use sprig_common::ids::ObjectId;
use sprig_common::{Result, SprigError};

use super::{AutoIndexStore, parse_origin};
use crate::autoindex::model::Origin;
use crate::sql::{AT, col, json_req, map_err, micros, row_count, time_col, to_json};

/// `index_usage.state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum UserIndexState {
    /// In use; judged every `usage_sample_reads` reads.
    Serving,
    /// Judged removable while `retire_user_indexes = recommend`.
    Recommended,
    /// `IGNORED`, watched for `usage_sample_reads` reads.
    Retiring,
    /// Dropped; the row keeps its definition for `sprigctl index restore`.
    Dropped,
    /// Restored or pinned by an operator: never judged.
    Pinned,
}

impl UserIndexState {
    /// The SQL text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            UserIndexState::Serving => "serving",
            UserIndexState::Recommended => "recommended",
            UserIndexState::Retiring => "retiring",
            UserIndexState::Dropped => "dropped",
            UserIndexState::Pinned => "pinned",
        }
    }

    /// Parses the SQL text.
    ///
    /// # Errors
    /// `Internal` for any other text.
    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "serving" => UserIndexState::Serving,
            "recommended" => UserIndexState::Recommended,
            "retiring" => UserIndexState::Retiring,
            "dropped" => UserIndexState::Dropped,
            "pinned" => UserIndexState::Pinned,
            other => {
                return Err(SprigError::internal(format!(
                    "index_usage: bad state {other:?}"
                )));
            }
        })
    }
}

impl std::fmt::Display for UserIndexState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An `index_usage` row.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageRow {
    /// The table.
    pub object: ObjectId,
    /// The index name in MariaDB.
    pub index_name: String,
    /// `user` or `migrated`.
    pub origin: Origin,
    /// The state.
    pub state: UserIndexState,
    /// The index's line of `SHOW CREATE TABLE`, as
    /// ``KEY `idx_b` (`b`)``: what `restore` adds back.
    pub definition: String,
    /// `{usage, retire}`, as for automatic indexes.
    pub observed: serde_json::Value,
    /// `unused` while retiring or dropped; `drift` when the user dropped it
    /// while it was ignored.
    pub drop_reason: Option<String>,
    /// First judged.
    pub created_at: SystemTime,
    /// Last state change.
    pub state_changed_at: SystemTime,
}

const COLS: &str = "object_id, index_name, origin, state, definition, observed, drop_reason, \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', created_at), \
    TIMESTAMPDIFF(MICROSECOND, '1970-01-01', state_changed_at)";

fn usage_row(mut r: Row) -> Result<UsageRow> {
    let origin: String = col(&mut r, 2, "origin")?;
    let state: String = col(&mut r, 3, "state")?;
    Ok(UsageRow {
        object: ObjectId(col(&mut r, 0, "object_id")?),
        index_name: col(&mut r, 1, "index_name")?,
        origin: parse_origin(&origin)?,
        state: UserIndexState::parse(&state)?,
        definition: col(&mut r, 4, "definition")?,
        observed: json_req(&mut r, 5, "observed")?,
        drop_reason: col(&mut r, 6, "drop_reason")?,
        created_at: time_col(&mut r, 7, "created_at")?,
        state_changed_at: time_col(&mut r, 8, "state_changed_at")?,
    })
}

impl AutoIndexStore {
    /// The `index_usage` rows of `object`, or of every object (`None`).
    ///
    /// # Errors
    /// 39070 or a backend error; `Internal` for a malformed row.
    pub async fn usage_rows<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: Option<ObjectId>,
    ) -> Result<Vec<UsageRow>> {
        let rows: Vec<Row> = match object {
            Some(o) => q
                .exec(
                    format!(
                        "SELECT {COLS} FROM {} WHERE object_id = ? ORDER BY index_name",
                        self.usage
                    ),
                    (o.0,),
                )
                .await
                .map_err(map_err)?,
            None => q
                .query(format!(
                    "SELECT {COLS} FROM {} ORDER BY object_id, index_name",
                    self.usage
                ))
                .await
                .map_err(map_err)?,
        };
        rows.into_iter().map(usage_row).collect()
    }

    /// Objects with a row in one of `states`.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn usage_objects_in<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        states: &[UserIndexState],
    ) -> Result<Vec<ObjectId>> {
        if states.is_empty() {
            return Ok(Vec::new());
        }
        let list = states
            .iter()
            .map(|s| format!("'{}'", s.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        let ids: Vec<u64> = q
            .query(format!(
                "SELECT DISTINCT object_id FROM {} WHERE state IN ({list})",
                self.usage
            ))
            .await
            .map_err(map_err)?;
        Ok(ids.into_iter().map(ObjectId).collect())
    }

    /// Inserts `r`, or replaces the row of the same index entirely.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn put_usage<Q: Queryable + Send>(&self, q: &mut Q, r: &UsageRow) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (object_id, index_name, origin, state, definition, observed, \
                 drop_reason, created_at, state_changed_at) VALUES (?, ?, ?, ?, ?, ?, ?, {AT}, {AT}) \
                 ON DUPLICATE KEY UPDATE origin = VALUES(origin), state = VALUES(state), \
                 definition = VALUES(definition), observed = VALUES(observed), \
                 drop_reason = VALUES(drop_reason), state_changed_at = VALUES(state_changed_at)",
                self.usage
            ),
            (
                r.object.0,
                &r.index_name,
                r.origin.as_str(),
                r.state.as_str(),
                &r.definition,
                to_json(&r.observed)?,
                r.drop_reason.as_deref(),
                micros(r.created_at),
                micros(r.state_changed_at),
            ),
        )
        .await
        .map_err(map_err)
    }

    /// Moves a row `from → to` if it is still in `from`, setting the drop
    /// reason (`None` clears it). Returns whether it moved.
    ///
    /// # Errors
    /// 39070 or a backend error.
    #[allow(clippy::too_many_arguments)]
    pub async fn set_usage_state<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        name: &str,
        from: UserIndexState,
        to: UserIndexState,
        drop_reason: Option<&str>,
        at: SystemTime,
    ) -> Result<bool> {
        q.exec_drop(
            format!(
                "UPDATE {} SET state = ?, drop_reason = ?, state_changed_at = {AT} \
                 WHERE object_id = ? AND index_name = ? AND state = ?",
                self.usage
            ),
            (
                to.as_str(),
                drop_reason,
                micros(at),
                object.0,
                name,
                from.as_str(),
            ),
        )
        .await
        .map_err(map_err)?;
        Ok(row_count(q).await? == 1)
    }

    /// Replaces a row's `observed`.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn set_usage_observed<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        name: &str,
        observed: &serde_json::Value,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET observed = ? WHERE object_id = ? AND index_name = ?",
                self.usage
            ),
            (to_json(observed)?, object.0, name),
        )
        .await
        .map_err(map_err)
    }

    /// Forgets a row (the user dropped the index). Returns whether one went.
    ///
    /// # Errors
    /// 39070 or a backend error.
    pub async fn delete_usage<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        object: ObjectId,
        name: &str,
    ) -> Result<bool> {
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE object_id = ? AND index_name = ?",
                self.usage
            ),
            (object.0, name),
        )
        .await
        .map_err(map_err)?;
        Ok(row_count(q).await? == 1)
    }
}
