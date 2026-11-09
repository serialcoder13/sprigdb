//! The `graph` catalog module (chapter 43, "Catalog tables"; build step
//! 2): `graphs`, `graph_node_sources` and `graph_edge_sources` in
//! `t<tenant>_sprig`, the snapshot state ([`GraphExtension`]), graph DDL
//! text, and the physical tables of a native graph (step 3).
//!
//! **As built:**
//! - Native graphs only. `CREATE PROPERTY GRAPH` (mapped graphs) and
//!   `ALTER GRAPH ... PARTITION BY` answer 39901.
//! - A native graph's tables live in its database's first shard schema
//!   (one shard, like queues and streams today); `part`, `_sprig_rk` and
//!   `dst_rk` are filled, so placing them in a table group later needs no
//!   data change.
//! - `node_seq` and `edge_seq` are NULL: ids come from a counter table
//!   `__sprig_gq_<graph>` in the same schema, handed out in blocks.
//! - A graph is an `objects` row of kind `graph` in namespace `graph`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use sprig_catalog::CatalogExtension;
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::migrate::{Migration, Phase};
use sprig_catalog::version::VersionKind;
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};

/// The module name.
pub const MODULE: &str = "graph";

const DDL_GRAPHS: &str = "CREATE TABLE IF NOT EXISTS graphs (
  graph_id      BIGINT UNSIGNED NOT NULL,
  db_id         INT UNSIGNED NOT NULL,
  kind          ENUM('mapped','native') NOT NULL,
  group_id      INT UNSIGNED NULL,
  node_seq      BIGINT UNSIGNED NULL,
  edge_seq      BIGINT UNSIGNED NULL,
  partition_by  JSON NULL,
  mirror_state  ENUM('none','building','ready') NOT NULL DEFAULT 'none',
  state         ENUM('creating','active','repartitioning','dropping') NOT NULL,
  definition    MEDIUMTEXT NULL,
  def_version   BIGINT UNSIGNED NOT NULL,
  created_at    DATETIME(6) NOT NULL,
  updated_at    DATETIME(6) NOT NULL,
  PRIMARY KEY (graph_id), KEY k_db (db_id)
) ENGINE=InnoDB";

const DDL_NODE_SOURCES: &str = "CREATE TABLE IF NOT EXISTS graph_node_sources (
  graph_id      BIGINT UNSIGNED NOT NULL,
  source_id     INT UNSIGNED NOT NULL,
  kind          ENUM('table','collection','label') NOT NULL,
  object_id     BIGINT UNSIGNED NULL,
  label         VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
  extra_labels  JSON NULL,
  key_columns   JSON NULL,
  properties    JSON NULL,
  id_mode       ENUM('native','int_key','hashed') NOT NULL,
  writable      BOOLEAN NOT NULL,
  state         ENUM('active','dropping') NOT NULL,
  native_label  VARCHAR(255) COLLATE utf8mb4_bin
                AS (IF(kind = 'label', label, NULL)) PERSISTENT,
  PRIMARY KEY (graph_id, source_id),
  UNIQUE KEY uk_native_label (graph_id, native_label)
) ENGINE=InnoDB";

const DDL_EDGE_SOURCES: &str = "CREATE TABLE IF NOT EXISTS graph_edge_sources (
  graph_id      BIGINT UNSIGNED NOT NULL,
  source_id     INT UNSIGNED NOT NULL,
  kind          ENUM('table','collection','type') NOT NULL,
  object_id     BIGINT UNSIGNED NULL,
  type          VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
  key_columns   JSON NULL,
  src_source_id INT UNSIGNED NULL,
  src_columns   JSON NULL,
  dst_source_id INT UNSIGNED NULL,
  dst_columns   JSON NULL,
  properties    JSON NULL,
  writable      BOOLEAN NOT NULL,
  state         ENUM('active','dropping') NOT NULL,
  native_type   VARCHAR(255) COLLATE utf8mb4_bin
                AS (IF(kind = 'type', type, NULL)) PERSISTENT,
  PRIMARY KEY (graph_id, source_id),
  UNIQUE KEY uk_native_type (graph_id, native_type)
) ENGINE=InnoDB";

/// The module's migrations.
pub static GRAPH_MIGRATIONS: &[Migration] = &[Migration {
    format: 1,
    phase: Phase::Expand,
    name: "graphs, graph_node_sources, graph_edge_sources",
    statements: &[DDL_GRAPHS, DDL_NODE_SOURCES, DDL_EDGE_SOURCES],
}];

fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            SprigError::backend(
                s.code,
                state,
                format!("graph statement failed ({}): {}", s.code, s.message),
            )
        }
        _ => SprigError::new(SprigCode::CatalogUnavailable, "graph storage unavailable"),
    }
    .with_source(e)
}

/// A MariaDB error as a `SprigError`.
#[must_use]
pub fn backend_error(e: mysql_async::Error) -> SprigError {
    map_err(e)
}

/// One graph as the catalog has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDef {
    /// `objects.object_id`.
    pub graph_id: u64,
    /// The database.
    pub db_id: u32,
    /// `native` (mapped graphs aren't built).
    pub kind: String,
    /// `creating`, `active`, ...
    pub state: String,
    /// Label ids to names.
    pub labels: BTreeMap<u32, String>,
    /// Type ids to names.
    pub types: BTreeMap<u32, String>,
}

/// The module's state in a snapshot.
#[derive(Debug, Clone, Default)]
pub struct GraphState {
    /// Graphs by id.
    pub graphs: HashMap<u64, Arc<GraphDef>>,
}

impl ExtState for GraphState {
    fn bytes(&self) -> usize {
        self.graphs
            .values()
            .map(|g| 64 + (g.labels.len() + g.types.len()) * 48)
            .sum()
    }
}

/// Reads and writes the module's tables in catalog schema `schema`.
#[derive(Debug, Clone)]
pub struct GraphCatalog {
    schema: String,
}

impl GraphCatalog {
    /// Over `t<tenant>_sprig`.
    #[must_use]
    pub fn new(schema: &str) -> Self {
        Self {
            schema: schema.to_owned(),
        }
    }

    fn t(&self, name: &str) -> String {
        format!("{}.{}", quote(&self.schema), name)
    }

    /// Whether the module's tables exist.
    ///
    /// # Errors
    /// A backend error.
    pub async fn migrated<Q: Queryable + Send>(&self, q: &mut Q) -> Result<bool> {
        let n: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'graph_edge_sources'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        Ok(n.unwrap_or(0) > 0)
    }

    /// Creates the module's tables (idempotent).
    ///
    /// # Errors
    /// A backend error.
    pub async fn migrate(&self, q: &mut mysql_async::Conn) -> Result<()> {
        sprig_catalog::Migrator::new(&[sprig_catalog::migrate::Module {
            name: MODULE,
            migrations: GRAPH_MIGRATIONS,
        }])?
        .run(
            q,
            &self.schema,
            &sprig_catalog::migrate::MigrateOpts::default(),
        )
        .await?;
        Ok(())
    }

    /// Every graph.
    ///
    /// # Errors
    /// A backend error.
    pub async fn load<Q: Queryable + Send>(&self, q: &mut Q) -> Result<GraphState> {
        let rows: Vec<(u64, u32, String, String)> = q
            .query(format!(
                "SELECT graph_id, db_id, kind, state FROM {}",
                self.t("graphs")
            ))
            .await
            .map_err(map_err)?;
        let mut graphs: HashMap<u64, GraphDef> = rows
            .into_iter()
            .map(|(g, d, k, s)| {
                (
                    g,
                    GraphDef {
                        graph_id: g,
                        db_id: d,
                        kind: k,
                        state: s,
                        labels: BTreeMap::new(),
                        types: BTreeMap::new(),
                    },
                )
            })
            .collect();
        let labels: Vec<(u64, u32, String)> = q
            .query(format!(
                "SELECT graph_id, source_id, label FROM {} WHERE kind = 'label'",
                self.t("graph_node_sources")
            ))
            .await
            .map_err(map_err)?;
        for (g, s, l) in labels {
            if let Some(d) = graphs.get_mut(&g) {
                d.labels.insert(s, l);
            }
        }
        let types: Vec<(u64, u32, String)> = q
            .query(format!(
                "SELECT graph_id, source_id, type FROM {} WHERE kind = 'type'",
                self.t("graph_edge_sources")
            ))
            .await
            .map_err(map_err)?;
        for (g, s, t) in types {
            if let Some(d) = graphs.get_mut(&g) {
                d.types.insert(s, t);
            }
        }
        Ok(GraphState {
            graphs: graphs.into_iter().map(|(k, v)| (k, Arc::new(v))).collect(),
        })
    }

    /// Inserts a native graph's row, `active`.
    ///
    /// # Errors
    /// A backend error.
    pub async fn insert_native<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        graph: u64,
        db: u32,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (graph_id, db_id, kind, state, def_version, created_at, updated_at) \
                 VALUES (?, ?, 'native', 'active', 1, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))",
                self.t("graphs")
            ),
            (graph, db),
        )
        .await
        .map_err(map_err)
    }

    /// Deletes a graph's rows.
    ///
    /// # Errors
    /// A backend error.
    pub async fn delete<Q: Queryable + Send>(&self, q: &mut Q, graph: u64) -> Result<()> {
        for t in ["graph_node_sources", "graph_edge_sources", "graphs"] {
            q.exec_drop(
                format!("DELETE FROM {} WHERE graph_id = ?", self.t(t)),
                (graph,),
            )
            .await
            .map_err(map_err)?;
        }
        Ok(())
    }

    /// The id of a native label (`label = true`) or relationship type.
    ///
    /// # Errors
    /// A backend error.
    pub async fn find_source<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        graph: u64,
        label: bool,
        name: &str,
    ) -> Result<Option<u32>> {
        let (table, native) = if label {
            ("graph_node_sources", "native_label")
        } else {
            ("graph_edge_sources", "native_type")
        };
        q.exec_first(
            format!(
                "SELECT source_id FROM {} WHERE graph_id = ? AND {native} = ?",
                self.t(table)
            ),
            (graph, name),
        )
        .await
        .map_err(map_err)
    }

    /// Adds a native label (`label = true`) or relationship type, or
    /// returns the one there: its id.
    ///
    /// # Errors
    /// A backend error.
    pub async fn add_source<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        graph: u64,
        label: bool,
        name: &str,
    ) -> Result<u32> {
        if name.is_empty() || name.len() > 255 {
            return Err(SprigError::new(
                SprigCode::GraphDefinitionInvalid,
                "a label or relationship type is 1 to 255 characters",
            ));
        }
        let (table, kind, col, native) = if label {
            ("graph_node_sources", "label", "label", "native_label")
        } else {
            ("graph_edge_sources", "type", "type", "native_type")
        };
        let extra = if label { ", id_mode" } else { "" };
        let extra_v = if label { ", 'native'" } else { "" };
        q.exec_drop(
            format!(
                "INSERT IGNORE INTO {t} (graph_id, source_id, kind, {col}, writable, state{extra}) \
                 SELECT ?, COALESCE(MAX(source_id), 0) + 1, '{kind}', ?, TRUE, 'active'{extra_v} \
                 FROM {t} WHERE graph_id = ?",
                t = self.t(table)
            ),
            (graph, name, graph),
        )
        .await
        .map_err(map_err)?;
        let id: Option<u32> = q
            .exec_first(
                format!(
                    "SELECT source_id FROM {} WHERE graph_id = ? AND {native} = ?",
                    self.t(table)
                ),
                (graph, name),
            )
            .await
            .map_err(map_err)?;
        id.ok_or_else(|| SprigError::internal("label row missing after insert"))
    }
}

/// The module in the catalog cache: [`GraphState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct GraphExtension;

#[async_trait::async_trait]
impl CatalogExtension for GraphExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        GRAPH_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        let c = GraphCatalog::new(tx.schema());
        let conn = tx.conn();
        if !c.migrated(conn).await? {
            return Ok(Arc::new(GraphState::default()));
        }
        Ok(Arc::new(c.load(conn).await?))
    }
}

// ----------------------------------------------------------------------
// Physical tables.
// ----------------------------------------------------------------------

/// Names of a native graph's tables.
#[derive(Debug, Clone)]
pub struct Tables {
    /// Nodes.
    pub nodes: String,
    /// Edges.
    pub edges: String,
    /// Label rows.
    pub labels: String,
    /// Id counters.
    pub seq: String,
}

impl Tables {
    /// The tables of graph `g` in `schema`, quoted and qualified.
    #[must_use]
    pub fn of(schema: &str, g: u64) -> Self {
        let q = |p: &str| format!("{}.{}", quote(schema), quote(&format!("__sprig_{p}_{g}")));
        Self {
            nodes: q("gn"),
            edges: q("ge"),
            labels: q("gl"),
            seq: q("gq"),
        }
    }

    /// `CREATE TABLE IF NOT EXISTS` for all four, and the counter rows.
    #[must_use]
    pub fn create_ddl(&self) -> Vec<String> {
        vec![
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
  id BIGINT UNSIGNED NOT NULL,
  part BIGINT UNSIGNED NOT NULL,
  labels JSON NOT NULL,
  props JSON NOT NULL, _types JSON NULL,
  _v BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (id), KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
                self.nodes
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
  src BIGINT UNSIGNED NOT NULL, type INT UNSIGNED NOT NULL,
  dst BIGINT UNSIGNED NOT NULL, id BIGINT UNSIGNED NOT NULL,
  dst_rk BIGINT UNSIGNED NOT NULL,
  props JSON NOT NULL, _types JSON NULL, _v BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (src, type, dst, id),
  UNIQUE KEY uk_id (id),
  KEY rev (dst, type, src),
  KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
                self.edges
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
  label INT UNSIGNED NOT NULL, id BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (label, id), KEY k_id (id)
) ENGINE=InnoDB",
                self.labels
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
  kind TINYINT UNSIGNED NOT NULL, next BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (kind)
) ENGINE=InnoDB",
                self.seq
            ),
            format!(
                "INSERT IGNORE INTO {} (kind, next) VALUES (1, 1), (2, 1)",
                self.seq
            ),
        ]
    }

    /// `DROP TABLE IF EXISTS` for all four.
    #[must_use]
    pub fn drop_ddl(&self) -> String {
        format!(
            "DROP TABLE IF EXISTS {}, {}, {}, {}",
            self.nodes, self.edges, self.labels, self.seq
        )
    }
}

// ----------------------------------------------------------------------
// Graph DDL text (SQL side).
// ----------------------------------------------------------------------

/// A graph DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphDdl {
    /// `CREATE GRAPH [IF NOT EXISTS] [db.]name`.
    Create {
        /// The database, if named.
        db: Option<String>,
        /// The graph.
        name: String,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
    },
    /// `DROP GRAPH [IF EXISTS] [db.]name`.
    Drop {
        /// The database, if named.
        db: Option<String>,
        /// The graph.
        name: String,
        /// `IF EXISTS`.
        if_exists: bool,
    },
}

fn words(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for c in s.trim().trim_end_matches(';').chars() {
        if c == '`' {
            quoted = !quoted;
            cur.push(c);
        } else if !quoted && c.is_whitespace() {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
        } else {
            cur.push(c);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn unquote(s: &str) -> String {
    s.strip_prefix('`')
        .and_then(|x| x.strip_suffix('`'))
        .unwrap_or(s)
        .to_owned()
}

/// Splits `db.name` (either part may be back-quoted).
fn qualified(s: &str) -> Result<(Option<String>, String)> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for c in s.chars() {
        match c {
            '`' => quoted = !quoted,
            '.' if !quoted => parts.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    parts.push(cur);
    let bad = || {
        SprigError::new(
            SprigCode::GraphDefinitionInvalid,
            format!("'{s}' is not a valid graph name"),
        )
    };
    let check = |n: &str| -> Result<()> {
        if n.is_empty()
            || n.len() > 64
            || n.to_ascii_lowercase().starts_with("__sprig_")
            || n.chars().any(char::is_control)
        {
            return Err(bad());
        }
        Ok(())
    };
    match parts.as_slice() {
        [n] => {
            check(n)?;
            Ok((None, n.clone()))
        }
        [d, n] => {
            check(d)?;
            check(n)?;
            Ok((Some(d.clone()), n.clone()))
        }
        _ => Err(bad()),
    }
}

/// Recognizes graph DDL in a SQL text: `None` if it isn't graph DDL.
///
/// # Errors
/// 39900 for malformed graph DDL; 39901 for `CREATE PROPERTY GRAPH` and
/// `ALTER GRAPH`, which this build doesn't have.
pub fn parse_ddl(sql: &str) -> Result<Option<GraphDdl>> {
    let w = words(sql);
    let up: Vec<String> = w.iter().map(|x| x.to_ascii_uppercase()).collect();
    let u: Vec<&str> = up.iter().map(String::as_str).collect();
    let syntax = || {
        SprigError::new(
            SprigCode::GraphSyntax,
            format!("malformed graph statement: {sql}"),
        )
    };
    match u.as_slice() {
        ["CREATE", "PROPERTY", "GRAPH", ..]
        | ["CREATE", "OR", "REPLACE", "PROPERTY", "GRAPH", ..] => Err(SprigError::new(
            SprigCode::GraphUnsupported,
            "CREATE PROPERTY GRAPH (mapped graphs) is not supported yet; use CREATE GRAPH for a native graph",
        )),
        ["ALTER", "GRAPH", ..] => Err(SprigError::new(
            SprigCode::GraphUnsupported,
            "ALTER GRAPH (partitioning) is not supported yet",
        )),
        ["CREATE", "GRAPH", "IF", "NOT", "EXISTS", _] => {
            let (db, name) = qualified(&w[5])?;
            Ok(Some(GraphDdl::Create {
                db,
                name: unquote(&name),
                if_not_exists: true,
            }))
        }
        ["CREATE", "GRAPH", _] | ["CREATE", "GRAPH", _, "ANY"] => {
            let (db, name) = qualified(&w[2])?;
            Ok(Some(GraphDdl::Create {
                db,
                name,
                if_not_exists: false,
            }))
        }
        ["DROP", "GRAPH", "IF", "EXISTS", _] => {
            let (db, name) = qualified(&w[4])?;
            Ok(Some(GraphDdl::Drop {
                db,
                name,
                if_exists: true,
            }))
        }
        ["DROP", "GRAPH", _] | ["DROP", "PROPERTY", "GRAPH", _] => {
            let (db, name) = qualified(w.last().map_or("", String::as_str))?;
            Ok(Some(GraphDdl::Drop {
                db,
                name,
                if_exists: false,
            }))
        }
        ["CREATE" | "DROP", "GRAPH", ..] => Err(syntax()),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_is_well_formed() {
        sprig_catalog::Migrator::new(&[sprig_catalog::migrate::Module {
            name: MODULE,
            migrations: GRAPH_MIGRATIONS,
        }])
        .unwrap_or_else(|_| unreachable!());
    }

    #[test]
    fn ddl_text() {
        assert_eq!(
            parse_ddl("CREATE GRAPH social").expect("ok"),
            Some(GraphDdl::Create {
                db: None,
                name: "social".into(),
                if_not_exists: false
            })
        );
        assert_eq!(
            parse_ddl("create graph if not exists shop.`my graph`;").expect("ok"),
            Some(GraphDdl::Create {
                db: Some("shop".into()),
                name: "my graph".into(),
                if_not_exists: true
            })
        );
        assert_eq!(
            parse_ddl("DROP GRAPH IF EXISTS g").expect("ok"),
            Some(GraphDdl::Drop {
                db: None,
                name: "g".into(),
                if_exists: true
            })
        );
        assert_eq!(parse_ddl("SELECT 1").expect("ok"), None);
        assert_eq!(
            parse_ddl("CREATE PROPERTY GRAPH shop VERTEX TABLES (x)")
                .expect_err("mapped")
                .code(),
            Some(SprigCode::GraphUnsupported)
        );
        assert_eq!(
            parse_ddl("CREATE GRAPH __sprig_x")
                .expect_err("reserved")
                .code(),
            Some(SprigCode::GraphDefinitionInvalid)
        );
        assert_eq!(
            parse_ddl("CREATE GRAPH a b c").expect_err("bad").code(),
            Some(SprigCode::GraphSyntax)
        );
        let t = Tables::of("t1_d2_g3_s1", 901);
        assert!(t.nodes.ends_with("`__sprig_gn_901`"));
        assert_eq!(t.create_ddl().len(), 5);
    }
}
