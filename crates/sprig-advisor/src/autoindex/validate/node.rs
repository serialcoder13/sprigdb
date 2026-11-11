//! Statements the validators run on a node: the candidate's DDL, the
//! engine-independent statistics snapshot that keeps the node exactly as it
//! was, the table signature that detects user DDL, and GTID positions.

use std::collections::BTreeMap;
use std::time::Duration;

use sprig_cluster::sql::{NodeEndpoint, ProbeError, ProbeSession, TextRow, literal};
use sprig_common::names::quote;

use super::IndexTarget;

/// A connection to the node a validation runs on.
pub(crate) struct NodeSql {
    pub(crate) s: ProbeSession,
}

/// The value of column `name` in `row`.
pub(crate) fn col<'a>(row: &'a TextRow, name: &str) -> Option<&'a str> {
    row.iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(name))
        .and_then(|(_, v)| v.as_deref())
}

impl NodeSql {
    pub(crate) async fn connect(ep: &NodeEndpoint, timeout: Duration) -> Result<Self, ProbeError> {
        Ok(Self {
            s: ProbeSession::connect(ep, timeout).await?,
        })
    }

    pub(crate) async fn scalar(&mut self, sql: &str) -> Result<Option<String>, ProbeError> {
        self.s.scalar(sql).await
    }

    pub(crate) async fn exec(&mut self, sql: &str) -> Result<(), ProbeError> {
        self.s.exec(sql).await
    }

    pub(crate) async fn rows(&mut self, sql: &str) -> Result<Vec<TextRow>, ProbeError> {
        self.s.rows(sql).await
    }

    /// `SHOW GLOBAL STATUS` variables by name.
    pub(crate) async fn status(
        &mut self,
        names: &[&str],
    ) -> Result<BTreeMap<String, String>, ProbeError> {
        let list = names
            .iter()
            .map(|n| literal(n))
            .collect::<Vec<_>>()
            .join(", ");
        let rows = self
            .rows(&format!(
                "SHOW GLOBAL STATUS WHERE Variable_name IN ({list})"
            ))
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                Some((
                    col(r, "Variable_name")?.to_ascii_lowercase(),
                    col(r, "Value").unwrap_or_default().to_owned(),
                ))
            })
            .collect())
    }

    /// Whether the index exists, and its columns in order.
    pub(crate) async fn index_columns(
        &mut self,
        t: &IndexTarget,
        index: &str,
    ) -> Result<Vec<String>, ProbeError> {
        let rows = self
            .rows(&format!(
                "SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = {} \
                 AND TABLE_NAME = {} AND INDEX_NAME = {} ORDER BY SEQ_IN_INDEX",
                literal(&t.schema),
                literal(&t.table),
                literal(index)
            ))
            .await?;
        Ok(rows
            .iter()
            .filter_map(|r| col(r, "COLUMN_NAME").map(str::to_owned))
            .collect())
    }

    /// Which of `columns` the table lacks.
    pub(crate) async fn missing_columns(
        &mut self,
        t: &IndexTarget,
        columns: &[String],
    ) -> Result<Vec<String>, ProbeError> {
        if columns.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .rows(&format!(
                "SELECT LOWER(COLUMN_NAME) AS c FROM information_schema.COLUMNS WHERE                  TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                literal(&t.schema),
                literal(&t.table)
            ))
            .await?;
        let have: Vec<&str> = rows.iter().filter_map(|r| col(r, "c")).collect();
        Ok(columns
            .iter()
            .filter(|c| !have.contains(&c.to_ascii_lowercase().as_str()))
            .cloned()
            .collect())
    }

    /// The table's columns and indexes except `skip`: a change means user
    /// DDL ran during the validation. Field columns (`_sprig_p_*`) don't
    /// count: a field-column candidate adds its own (R6 round 6).
    pub(crate) async fn table_signature(
        &mut self,
        t: &IndexTarget,
        skip: &str,
    ) -> Result<String, ProbeError> {
        let cols = self
            .scalar(&format!(
                "SELECT GROUP_CONCAT(CONCAT_WS(':', COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE) ORDER BY \
                 ORDINAL_POSITION SEPARATOR ',') FROM information_schema.COLUMNS WHERE \
                 TABLE_SCHEMA = {} AND TABLE_NAME = {} AND COLUMN_NAME NOT LIKE '\\_sprig\\_p\\_%'",
                literal(&t.schema),
                literal(&t.table)
            ))
            .await?
            .unwrap_or_default();
        let idx = self
            .scalar(&format!(
                "SELECT GROUP_CONCAT(CONCAT_WS(':', INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, \
                 IFNULL(SUB_PART, '')) ORDER BY INDEX_NAME, SEQ_IN_INDEX SEPARATOR ',') FROM \
                 information_schema.STATISTICS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} AND \
                 INDEX_NAME <> {}",
                literal(&t.schema),
                literal(&t.table),
                literal(skip)
            ))
            .await?
            .unwrap_or_default();
        Ok(format!("{cols}|{idx}"))
    }

    /// `@@server_id` and `@@gtid_binlog_pos`.
    pub(crate) async fn gtid(&mut self) -> Result<(u32, String), ProbeError> {
        let rows = self
            .rows("SELECT @@server_id AS id, @@gtid_binlog_pos AS pos")
            .await?;
        let r = rows.first().ok_or_else(|| ProbeError {
            errno: None,
            message: "no row".into(),
            timeout: false,
        })?;
        Ok((
            col(r, "id").and_then(|v| v.parse().ok()).unwrap_or(0),
            col(r, "pos").unwrap_or_default().to_owned(),
        ))
    }
}

/// Watches a node while a long statement runs on it.
#[async_trait::async_trait]
pub(crate) trait NodeWatch: Send {
    /// `Err(reason)` when the node or cluster is no longer safe.
    async fn check(&mut self) -> Result<(), String>;
    /// Kills the running statement of connection `id`.
    async fn kill_query(&mut self, id: u32) -> Result<(), ProbeError>;
}

/// Runs `sql` on `ddl`, checking `watch` every `poll`; when a check fails
/// the statement is killed (`KILL QUERY`) and the reason returned with the
/// statement's result.
pub(crate) async fn run_watched(
    ddl: &mut NodeSql,
    watch: &mut dyn NodeWatch,
    sql: &str,
    poll: Duration,
) -> (Result<(), ProbeError>, Option<String>) {
    let id = ddl.s.connection_id();
    let stmt = ddl.s.exec(sql);
    tokio::pin!(stmt);
    let mut killed: Option<String> = None;
    loop {
        tokio::select! {
            res = &mut stmt => return (res, killed),
            () = tokio::time::sleep(poll) => {
                if killed.is_none()
                    && let Err(why) = watch.check().await
                {
                    tracing::warn!(reason = %why, "killing the validation DDL");
                    if let Err(e) = watch.kill_query(id).await {
                        tracing::error!(error = %e, "KILL QUERY failed");
                    }
                    killed = Some(why);
                }
            }
        }
    }
}

/// Snapshots compare by their rows.
impl PartialEq for StatsSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.tables.len() == other.tables.len()
            && self
                .tables
                .iter()
                .zip(&other.tables)
                .all(|(a, b)| a.0 == b.0 && a.2 == b.2)
    }
}

/// `ALTER TABLE ... ADD INDEX IF NOT EXISTS name (cols)`, online.
pub(crate) fn add_index_sql(t: &IndexTarget, name: &str, columns: &str, online: bool) -> String {
    format!(
        "ALTER TABLE {}.{} ADD INDEX IF NOT EXISTS {} ({columns}){}",
        quote(&t.schema),
        quote(&t.table),
        quote(name),
        if online {
            ", ALGORITHM=INPLACE, LOCK=NONE"
        } else {
            ""
        }
    )
}

/// [`add_index_sql`] for any kind of `def`, as `name` (chapter 13,
/// "Other models"; R6 round 3): spatial and vector indexes are added
/// without `ALGORITHM=INPLACE, LOCK=NONE`, which MariaDB refuses for them.
pub(crate) fn add_def_sql(
    t: &IndexTarget,
    name: &str,
    def: &crate::autoindex::model::IndexDef,
    online: bool,
) -> String {
    // Field columns (R6 round 6) are added first, by
    // [`add_fields_sql`]; their index builds online like a B-tree.
    if matches!(
        def.kind,
        crate::autoindex::model::IndexKind::BTree | crate::autoindex::model::IndexKind::FieldColumn
    ) {
        return add_index_sql(t, name, &def.ddl_columns(), online);
    }
    format!(
        "ALTER TABLE {}.{} ADD {}",
        quote(&t.schema),
        quote(&t.table),
        def.add_clause_named(name)
    )
}

/// The field columns of `def` (R6 round 6, decision K3), added before
/// its index: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...,
/// ALGORITHM=INSTANT`; `None` for other kinds.
pub(crate) fn add_fields_sql(
    t: &IndexTarget,
    def: &crate::autoindex::model::IndexDef,
) -> Option<String> {
    crate::autoindex::kv::add_fields_sql(&t.schema, &t.table, def)
}

/// `ALTER TABLE ... DROP INDEX IF EXISTS name`.
pub(crate) fn drop_index_sql(t: &IndexTarget, name: &str) -> String {
    format!(
        "ALTER TABLE {}.{} DROP INDEX IF EXISTS {}",
        quote(&t.schema),
        quote(&t.table),
        quote(name)
    )
}

/// `ANALYZE LOCAL TABLE ... PERSISTENT FOR COLUMNS (...) INDEXES (x)`:
/// statistics for the new index and its columns, on this node only.
pub(crate) fn analyze_sql(t: &IndexTarget, name: &str, columns: &[String]) -> String {
    format!(
        "ANALYZE LOCAL TABLE {}.{} PERSISTENT FOR COLUMNS ({}) INDEXES ({})",
        quote(&t.schema),
        quote(&t.table),
        columns
            .iter()
            .map(|c| quote(c))
            .collect::<Vec<_>>()
            .join(", "),
        quote(name)
    )
}

/// The engine-independent statistics rows `ANALYZE ... PERSISTENT` may
/// write, as they were before: `mysql.table_stats` for the table,
/// `mysql.column_stats` for the columns and `mysql.index_stats` for the
/// index. Values are kept hex-encoded so binary histograms survive.
#[derive(Debug, Clone, Default)]
pub(crate) struct StatsSnapshot {
    tables: Vec<StatRows>,
}

/// One statistics table's rows: the table, its columns, the rows (hex),
/// and the `WHERE` that selects them.
type StatRows = (&'static str, Vec<String>, Vec<Vec<Option<String>>>, String);

const STAT_TABLES: [&str; 3] = ["table_stats", "column_stats", "index_stats"];

fn stats_where(stat: &str, t: &IndexTarget, columns: &[String], index: &str) -> String {
    let base = format!(
        "db_name = {} AND table_name = {}",
        literal(&t.schema),
        literal(&t.table)
    );
    match stat {
        "column_stats" => format!(
            "{base} AND column_name IN ({})",
            columns
                .iter()
                .map(|c| literal(c))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        "index_stats" => format!("{base} AND index_name = {}", literal(index)),
        _ => base,
    }
}

impl StatsSnapshot {
    /// Reads the rows. `SELECT` on the three tables is needed.
    pub(crate) async fn take(
        n: &mut NodeSql,
        t: &IndexTarget,
        columns: &[String],
        index: &str,
    ) -> Result<Self, ProbeError> {
        let mut out = Self::default();
        for stat in STAT_TABLES {
            let names: Vec<String> = n
                .rows(&format!(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = \
                     'mysql' AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
                    literal(stat)
                ))
                .await?
                .iter()
                .filter_map(|r| col(r, "COLUMN_NAME").map(str::to_owned))
                .collect();
            if names.is_empty() {
                continue;
            }
            let select = names
                .iter()
                .map(|c| format!("HEX(CAST({} AS BINARY)) AS {}", quote(c), quote(c)))
                .collect::<Vec<_>>()
                .join(", ");
            let filter = stats_where(stat, t, columns, index);
            let rows = n
                .rows(&format!("SELECT {select} FROM mysql.{stat} WHERE {filter}"))
                .await?;
            let values = rows
                .iter()
                .map(|r| names.iter().map(|c| col(r, c).map(str::to_owned)).collect())
                .collect();
            out.tables.push((stat, names, values, filter));
        }
        Ok(out)
    }

    /// Puts the rows back as they were, with the binary log off for the
    /// session (`DELETE` and `INSERT` on `mysql.*_stats` needed). The
    /// statistics tables are Aria, so Galera doesn't replicate them either.
    pub(crate) async fn restore(&self, n: &mut NodeSql) -> Result<(), ProbeError> {
        for (stat, names, rows, filter) in &self.tables {
            n.exec(&format!("DELETE FROM mysql.{stat} WHERE {filter}"))
                .await?;
            for r in rows {
                let vals = r
                    .iter()
                    .map(|v| {
                        v.as_ref()
                            .map_or("NULL".to_owned(), |h| format!("UNHEX('{h}')"))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let cols = names
                    .iter()
                    .map(|c| quote(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                n.exec(&format!(
                    "INSERT INTO mysql.{stat} ({cols}) VALUES ({vals})"
                ))
                .await?;
            }
        }
        Ok(())
    }

    /// Rows held.
    pub(crate) fn rows(&self) -> usize {
        self.tables.iter().map(|t| t.2.len()).sum()
    }
}

/// A GTID position as `domain → (server_id, seq)`.
pub(crate) fn parse_gtid_pos(pos: &str) -> BTreeMap<u32, (u32, u64)> {
    pos.split(',')
        .filter_map(|g| {
            let mut it = g.trim().split('-');
            let d = it.next()?.parse().ok()?;
            let s = it.next()?.parse().ok()?;
            let q = it.next()?.parse().ok()?;
            Some((d, (s, q)))
        })
        .collect()
}

/// GTIDs in `after` that this server wrote itself since `before`: errant
/// transactions on a replica.
pub(crate) fn own_new_gtids(server_id: u32, before: &str, after: &str) -> Vec<String> {
    let b = parse_gtid_pos(before);
    parse_gtid_pos(after)
        .into_iter()
        .filter(|(d, (s, q))| *s == server_id && b.get(d) != Some(&(*s, *q)))
        .map(|(d, (s, q))| format!("{d}-{s}-{q}"))
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn t() -> IndexTarget {
        IndexTarget {
            schema: "t42_d1_g3_s17".into(),
            table: "orders".into(),
            pk: vec!["id".into()],
        }
    }

    #[test]
    fn ddl_text() {
        assert_eq!(
            add_index_sql(
                &t(),
                "sprig_auto_5be0c2a1",
                "`customer_id`, `created_at`",
                true
            ),
            "ALTER TABLE `t42_d1_g3_s17`.`orders` ADD INDEX IF NOT EXISTS `sprig_auto_5be0c2a1` \
             (`customer_id`, `created_at`), ALGORITHM=INPLACE, LOCK=NONE"
        );
        assert_eq!(
            drop_index_sql(&t(), "sprig_auto_5be0c2a1"),
            "ALTER TABLE `t42_d1_g3_s17`.`orders` DROP INDEX IF EXISTS `sprig_auto_5be0c2a1`"
        );
        assert_eq!(
            analyze_sql(
                &t(),
                "sprig_auto_5be0c2a1",
                &["customer_id".into(), "created_at".into()]
            ),
            "ANALYZE LOCAL TABLE `t42_d1_g3_s17`.`orders` PERSISTENT FOR COLUMNS (`customer_id`, \
             `created_at`) INDEXES (`sprig_auto_5be0c2a1`)"
        );
    }

    #[test]
    fn errant_gtids_are_this_servers_new_ones() {
        assert!(own_new_gtids(203, "2-201-2882", "2-201-7410").is_empty());
        assert_eq!(
            own_new_gtids(203, "2-201-2882", "2-203-2883"),
            vec!["2-203-2883".to_owned()]
        );
        assert_eq!(
            own_new_gtids(203, "0-203-5,2-201-1", "0-203-6,2-201-9"),
            vec!["0-203-6".to_owned()]
        );
        assert!(own_new_gtids(203, "0-203-5,2-201-1", "0-203-5,2-201-9").is_empty());
        assert!(own_new_gtids(203, "", "").is_empty());
    }
}
