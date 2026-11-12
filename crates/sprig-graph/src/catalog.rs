//! The `graph` catalog module (chapter 43, "Catalog tables"; build step
//! 2): `graphs`, `graph_node_sources` and `graph_edge_sources` in
//! `t<tenant>_sprig`, the snapshot state ([`GraphExtension`]), graph DDL
//! text, and the physical tables of a native graph (step 3).
//!
//! **As built:**
//! - `CREATE PROPERTY GRAPH` makes a mapped graph (round 3, see
//!   [`crate::mapped`]); `ALTER GRAPH ... PARTITION BY` answers 39901.
//! - Since round 3 a native graph is a sharded table group (`group_id`,
//!   mirrors `ready`); graphs from rounds 1 and 2 keep their tables in the
//!   database's first shard schema.
//! - `node_seq` and `edge_seq` are NULL: ids come from a counter table
//!   `__sprig_gq_<graph>` in the same schema, handed out in blocks.
//! - A graph is an `objects` row of kind `graph` in namespace `graph`.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
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

/// The tenant's bookmark key (`kid` 1), shared by every gateway.
const DDL_KEYS: &str = "CREATE TABLE IF NOT EXISTS graph_keys (
  kid     INT UNSIGNED NOT NULL,
  secret  VARBINARY(64) NOT NULL,
  PRIMARY KEY (kid)
) ENGINE=InnoDB";

/// Property indexes and uniqueness constraints of native graphs: each
/// is generated columns `_sprig_i<index_id>_<k>` over `props` on the node
/// or edge table, with an index (`UNIQUE` for a constraint).
const DDL_INDEXES: &str = "CREATE TABLE IF NOT EXISTS graph_indexes (
  graph_id    BIGINT UNSIGNED NOT NULL,
  index_id    INT UNSIGNED NOT NULL,
  name        VARCHAR(255) COLLATE utf8mb4_bin NOT NULL,
  kind        ENUM('range','text') NOT NULL,
  is_unique   BOOLEAN NOT NULL,
  entity      ENUM('node','relationship') NOT NULL,
  source_id   INT UNSIGNED NOT NULL,
  properties  JSON NOT NULL,
  state       ENUM('building','online','dropping') NOT NULL,
  created_at  DATETIME(6) NOT NULL,
  PRIMARY KEY (graph_id, index_id),
  UNIQUE KEY uk_name (graph_id, name)
) ENGINE=InnoDB";

/// The module's migrations.
pub static GRAPH_MIGRATIONS: &[Migration] = &[
    Migration {
        format: 1,
        phase: Phase::Expand,
        name: "graphs, graph_node_sources, graph_edge_sources",
        statements: &[DDL_GRAPHS, DDL_NODE_SOURCES, DDL_EDGE_SOURCES],
    },
    Migration {
        format: 2,
        phase: Phase::Expand,
        name: "graph_keys, graph_indexes",
        statements: &[DDL_KEYS, DDL_INDEXES],
    },
    Migration {
        format: 3,
        phase: Phase::Expand,
        name: "graph_mirror_pos, graphs.mirror_mode",
        statements: &[crate::mirror::DDL_POS, DDL_MIRROR_MODE],
    },
];

/// Who keeps a grouped graph's mirrors (round 5): `sync`, the writers;
/// `starting`, still the writers while the worker's applier starts on the
/// graph; `async`, the applier (`graph.mirror = async`).
const DDL_MIRROR_MODE: &str = "ALTER TABLE graphs ADD COLUMN IF NOT EXISTS \
  mirror_mode ENUM('sync','starting','async') NOT NULL DEFAULT 'sync' AFTER mirror_state";

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
    /// The graph's sharded table group (round 3); `None`: its tables are
    /// in the database's first shard schema (graphs from rounds 1 and 2).
    pub group_id: Option<u32>,
    /// Mirror rows are kept for every edge (`ready`).
    pub mirrors: bool,
    /// The worker's applier keeps them (`mirror_mode = async`, round 5).
    pub mirror_async: bool,
    /// `mirror_mode`: `sync`, `starting` or `async` (round 5).
    pub mirror_mode: String,
    /// A mapped graph's definition ([`crate::mapped::MappedDef`] as JSON).
    pub definition: Option<String>,
    /// Label ids to names.
    pub labels: BTreeMap<u32, String>,
    /// Type ids to names.
    pub types: BTreeMap<u32, String>,
    /// Indexes and constraints.
    pub indexes: Vec<crate::store::IndexDef>,
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

/// A `graph_indexes` row.
type IndexRow = (u64, u32, String, String, bool, String, u32, String, String);

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
        type GraphRow = (
            u64,
            u32,
            String,
            String,
            Option<u32>,
            String,
            Option<String>,
        );
        // `mirror_mode` arrived with migration 3; a catalog without it
        // keeps every mirror synchronous.
        let rows: Vec<GraphRow> = match q
            .query(format!(
                "SELECT graph_id, db_id, kind, state, group_id, \
                 CONCAT(mirror_state, ':', mirror_mode), definition FROM {}",
                self.t("graphs")
            ))
            .await
        {
            Ok(r) => r,
            Err(mysql_async::Error::Server(e)) if e.code == 1054 => q
                .query(format!(
                    "SELECT graph_id, db_id, kind, state, group_id, mirror_state, definition FROM {}",
                    self.t("graphs")
                ))
                .await
                .map_err(map_err)?,
            Err(e) => return Err(map_err(e)),
        };
        let mut graphs: HashMap<u64, GraphDef> = rows
            .into_iter()
            .map(|(g, d, k, s, group, m, definition)| {
                (
                    g,
                    GraphDef {
                        graph_id: g,
                        db_id: d,
                        kind: k,
                        state: s,
                        group_id: group,
                        mirrors: m == "ready" || m.starts_with("ready:"),
                        mirror_async: m.ends_with(":async"),
                        mirror_mode: m.split_once(':').map_or("sync", |x| x.1).to_owned(),
                        definition,
                        labels: BTreeMap::new(),
                        types: BTreeMap::new(),
                        indexes: Vec::new(),
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
        for d in self.indexes(q, None).await? {
            if let Some(g) = graphs.get_mut(&d.0) {
                let mut def = d.1;
                let lookup = match def.entity {
                    crate::ast::Entity::Node => &g.labels,
                    crate::ast::Entity::Relationship => &g.types,
                };
                if let Some(t) = lookup.get(&def.token_id) {
                    def.token.clone_from(t);
                }
                g.indexes.push(def);
            }
        }
        Ok(GraphState {
            graphs: graphs.into_iter().map(|(k, v)| (k, Arc::new(v))).collect(),
        })
    }

    /// Indexes and constraints (of one graph, or all), with their token
    /// names left empty: `(graph, definition)`.
    ///
    /// # Errors
    /// A backend error.
    pub async fn indexes<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        graph: Option<u64>,
    ) -> Result<Vec<(u64, crate::store::IndexDef)>> {
        let exists: Option<u64> = q
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'graph_indexes'",
                (self.schema.as_str(),),
            )
            .await
            .map_err(map_err)?;
        if exists.unwrap_or(0) == 0 {
            return Ok(Vec::new());
        }
        let filter = graph.map_or(String::new(), |g| format!(" WHERE graph_id = {g}"));
        let rows: Vec<IndexRow> = q
            .query(format!(
                "SELECT graph_id, index_id, name, kind, is_unique, entity, source_id, properties, state \
                 FROM {}{filter} ORDER BY graph_id, index_id",
                self.t("graph_indexes")
            ))
            .await
            .map_err(map_err)?;
        Ok(rows
            .into_iter()
            .map(|(g, id, name, kind, unique, entity, src, props, state)| {
                (
                    g,
                    crate::store::IndexDef {
                        id,
                        name,
                        unique,
                        kind: if kind == "text" {
                            crate::ast::IndexKind::Text
                        } else {
                            crate::ast::IndexKind::Range
                        },
                        entity: if entity == "node" {
                            crate::ast::Entity::Node
                        } else {
                            crate::ast::Entity::Relationship
                        },
                        token: String::new(),
                        token_id: src,
                        props: serde_json::from_str::<Vec<String>>(&props).unwrap_or_default(),
                        state,
                    },
                )
            })
            .collect())
    }

    /// The tenant's bookmark secret, made on first use.
    ///
    /// # Errors
    /// A backend error.
    pub async fn bookmark_secret<Q: Queryable + Send>(&self, q: &mut Q) -> Result<Vec<u8>> {
        let mut secret = [0u8; 32];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut secret)
            .map_err(|_| SprigError::internal("no randomness for the bookmark key"))?;
        q.exec_drop(
            format!(
                "INSERT IGNORE INTO {} (kid, secret) VALUES (1, ?)",
                self.t("graph_keys")
            ),
            (secret.to_vec(),),
        )
        .await
        .map_err(map_err)?;
        let s: Option<Vec<u8>> = q
            .exec_first(
                format!("SELECT secret FROM {} WHERE kid = 1", self.t("graph_keys")),
                (),
            )
            .await
            .map_err(map_err)?;
        s.ok_or_else(|| SprigError::internal("no bookmark key"))
    }

    /// Adds index `d` of `graph` as `building`, with the next index id:
    /// that id.
    ///
    /// # Errors
    /// A backend error (1062 for a name in use).
    pub async fn insert_index<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        graph: u64,
        d: &crate::store::IndexDef,
    ) -> Result<u32> {
        let id: Option<u32> = q
            .exec_first(
                format!(
                    "SELECT COALESCE(MAX(index_id), 0) + 1 FROM {} WHERE graph_id = ?",
                    self.t("graph_indexes")
                ),
                (graph,),
            )
            .await
            .map_err(map_err)?;
        let id = id.unwrap_or(1);
        q.exec_drop(
            format!(
                "INSERT INTO {} (graph_id, index_id, name, kind, is_unique, entity, source_id, \
                 properties, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'building', UTC_TIMESTAMP(6))",
                self.t("graph_indexes")
            ),
            (
                graph,
                id,
                d.name.as_str(),
                if d.kind == crate::ast::IndexKind::Text {
                    "text"
                } else {
                    "range"
                },
                d.unique,
                if d.entity == crate::ast::Entity::Node {
                    "node"
                } else {
                    "relationship"
                },
                d.token_id,
                serde_json::to_string(&d.props).unwrap_or_default(),
            ),
        )
        .await
        .map_err(map_err)?;
        Ok(id)
    }

    /// Marks index `id` of `graph` `online`.
    ///
    /// # Errors
    /// A backend error.
    pub async fn index_online<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        graph: u64,
        id: u32,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET state = 'online' WHERE graph_id = ? AND index_id = ?",
                self.t("graph_indexes")
            ),
            (graph, id),
        )
        .await
        .map_err(map_err)
    }

    /// Deletes index `id` of `graph`.
    ///
    /// # Errors
    /// A backend error.
    pub async fn delete_index<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        graph: u64,
        id: u32,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "DELETE FROM {} WHERE graph_id = ? AND index_id = ?",
                self.t("graph_indexes")
            ),
            (graph, id),
        )
        .await
        .map_err(map_err)
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
        self.insert_native_in(q, graph, db, None).await
    }

    /// Inserts a mapped graph's row with its definition (JSON).
    ///
    /// # Errors
    /// A backend error.
    pub async fn insert_mapped<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        graph: u64,
        db: u32,
        definition: &str,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (graph_id, db_id, kind, definition, state, def_version, \
                 created_at, updated_at) \
                 VALUES (?, ?, 'mapped', ?, 'active', 1, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))",
                self.t("graphs")
            ),
            (graph, db, definition),
        )
        .await
        .map_err(map_err)
    }

    /// Inserts a native graph's row with its table group (`None`: one
    /// schema, no mirrors); a grouped graph keeps mirror rows from the
    /// start (`mirror_state = ready`).
    ///
    /// # Errors
    /// A backend error.
    pub async fn insert_native_in<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        graph: u64,
        db: u32,
        group: Option<u32>,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "INSERT INTO {} (graph_id, db_id, kind, group_id, mirror_state, state, def_version, \
                 created_at, updated_at) \
                 VALUES (?, ?, 'native', ?, ?, 'active', 1, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))",
                self.t("graphs")
            ),
            (graph, db, group, if group.is_some() { "ready" } else { "none" }),
        )
        .await
        .map_err(map_err)
    }

    /// Deletes a graph's rows.
    ///
    /// # Errors
    /// A backend error.
    pub async fn delete<Q: Queryable + Send>(&self, q: &mut Q, graph: u64) -> Result<()> {
        for t in [
            "graph_indexes",
            "graph_node_sources",
            "graph_edge_sources",
            "graphs",
        ] {
            q.exec_drop(
                format!("DELETE FROM {} WHERE graph_id = ?", self.t(t)),
                (graph,),
            )
            .await
            .map_err(map_err)?;
        }
        // The applier's positions (migration 3; absent before it).
        match crate::mirror::clear_pos(q, &self.schema, graph).await {
            Err(e) if matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1146, .. }) => {}
            r => r?,
        }
        Ok(())
    }

    /// Sets who keeps a graph's mirrors and their state (round 5):
    /// `mode` `sync`, `starting` or `async`, `state` `building` or
    /// `ready`.
    ///
    /// # Errors
    /// A backend error.
    pub async fn set_mirror<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        graph: u64,
        mode: &str,
        state: &str,
    ) -> Result<()> {
        q.exec_drop(
            format!(
                "UPDATE {} SET mirror_mode = ?, mirror_state = ?, updated_at = UTC_TIMESTAMP(6) \
                 WHERE graph_id = ?",
                self.t("graphs")
            ),
            (mode, state, graph),
        )
        .await
        .map_err(map_err)
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tables {
    /// Nodes.
    pub nodes: String,
    /// Edges.
    pub edges: String,
    /// Label rows.
    pub labels: String,
    /// Mirrors of incoming edges (on the destination's shard).
    pub mirrors: String,
    /// Id counters.
    pub seq: String,
    /// Uniqueness claims (decision G41), on the shard of each claim's hash.
    pub claims: String,
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
            mirrors: q("gm"),
            seq: q("gq"),
            claims: q("gu"),
        }
    }

    /// `CREATE TABLE IF NOT EXISTS` for all five, and the counter rows (a
    /// graph in one schema).
    #[must_use]
    pub fn create_ddl(&self) -> Vec<String> {
        let mut v = self.shard_ddl();
        v.extend(self.seq_ddl());
        v
    }

    /// The counter table and its rows (in the database's first shard
    /// schema: it has no `_sprig_rk`, so it never joins the graph's group).
    #[must_use]
    pub fn seq_ddl(&self) -> Vec<String> {
        vec![
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

    /// `CREATE TABLE IF NOT EXISTS` for the tables of one shard: nodes,
    /// edges, label rows and mirrors.
    #[must_use]
    pub fn shard_ddl(&self) -> Vec<String> {
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
  dst BIGINT UNSIGNED NOT NULL, type INT UNSIGNED NOT NULL,
  src BIGINT UNSIGNED NOT NULL, id BIGINT UNSIGNED NOT NULL,
  src_rk BIGINT UNSIGNED NOT NULL, _v BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (dst, type, src, id),
  KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
                self.mirrors
            ),
            self.claims_ddl(),
        ]
    }

    /// `CREATE TABLE IF NOT EXISTS` for the uniqueness claims of one shard
    /// (decision G41): a row per constraint and key, routed by the claim's
    /// hash (`schema::claim_rk`), naming the element that holds the key.
    #[must_use]
    pub fn claims_ddl(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
  idx INT UNSIGNED NOT NULL, k VARBINARY(600) NOT NULL,
  owner BIGINT UNSIGNED NOT NULL, owner_rk BIGINT UNSIGNED NOT NULL,
  _sprig_rk BIGINT UNSIGNED NULL INVISIBLE,
  PRIMARY KEY (idx, k), KEY _sprig_rk (_sprig_rk)
) ENGINE=InnoDB",
            self.claims
        )
    }

    /// The virtual column of property `k` of index `d`.
    #[must_use]
    pub fn index_column(d: &crate::store::IndexDef, k: usize) -> String {
        format!("_sprig_i{}_{k}", d.id)
    }

    /// The virtual column holding property `p`'s typed text, shared by
    /// the indexes on `p` (R8 round 10): `_sprig_p` and 16 hex digits of
    /// the name's SHA-256.
    #[must_use]
    pub fn property_column(p: &str) -> String {
        let d = ring::digest::digest(&ring::digest::SHA256, p.as_bytes());
        let mut s = String::from("_sprig_p");
        for b in &d.as_ref()[..8] {
            let _ = write!(s, "{b:02x}");
        }
        s
    }

    /// The SQL of property `p`'s typed text, as [`crate::schema`] says
    /// (texts over [`crate::schema::MAX_INLINE`] characters hashed); `p`
    /// was checked for quotes.
    fn typed_text(p: &str) -> String {
        let v = format!("JSON_VALUE(props, '$.\"{p}\"')");
        // G77 (R8 round 9): a value `_types` names (bytes, a temporal
        // value) is `t<type>:<text>`, as `schema::prop_index_value` says,
        // so it never equals a plain string's `s<text>`.
        // G19: an integral float inside the 64-bit range is its integer,
        // as `schema::numeric_key` says. Plain digits stay as they are
        // (no detour through a double); MariaDB calls `1e16` an INTEGER,
        // so both types take the same path. `JSON_VALUE` answers `1` for
        // `true`.
        let f = format!("({v} + 0e0)");
        let num = format!(
            "CONCAT('n', IF(INSTR({v}, 'e') + INSTR({v}, 'E') + INSTR({v}, '.') = 0, {v}, \
             IF(FLOOR({f}) = {f} AND ABS({f}) < 9223372036854775807e0, CAST({f} AS SIGNED), {v})))"
        );
        let t = format!(
            "(CASE JSON_TYPE(JSON_EXTRACT(props, '$.\"{p}\"')) \
             WHEN 'STRING' THEN CONCAT(IFNULL(CONCAT('t', JSON_VALUE(_types, '$.\"{p}\"'), ':'), 's'), {v}) \
             WHEN 'INTEGER' THEN {num} WHEN 'DOUBLE' THEN {num} \
             WHEN 'BOOLEAN' THEN IF({v} IN ('1', 'true'), 'btrue', 'bfalse') END)"
        );
        format!(
            "IF(CHAR_LENGTH({t}) <= {max}, {t}, CONCAT('h', SHA2({t}, 256)))",
            max = crate::schema::MAX_INLINE
        )
    }

    /// `ALTER TABLE` adding index `d`'s virtual columns and its index
    /// (`UNIQUE` for a constraint); see [`crate::schema`] for the values.
    ///
    /// # Errors
    /// 39908 for a property name with quotes or backslashes.
    pub fn index_ddl(&self, d: &crate::store::IndexDef) -> Result<String> {
        use crate::ast::{Entity, IndexKind};
        let (table, cond) = match d.entity {
            Entity::Node => (
                &self.nodes,
                format!("JSON_CONTAINS(labels, '{}')", d.token_id),
            ),
            Entity::Relationship => (&self.edges, format!("type = {}", d.token_id)),
        };
        let mut parts = Vec::new();
        let mut cols = Vec::new();
        for (k, p) in d.props.iter().enumerate() {
            if p.contains(['"', '\\', '\'', '`']) || p.chars().any(char::is_control) {
                return Err(SprigError::new(
                    SprigCode::GraphDefinitionInvalid,
                    format!(
                        "property '{p}' can't be indexed: quotes and backslashes aren't allowed in indexed property names"
                    ),
                ));
            }
            // R8 round 10 (LDBC, MariaDB 1117 at the 20th index): the
            // typed text of a property is one virtual column per property
            // (`_sprig_p<hash>`), shared by every index on it; each
            // index's own column only picks it for its label or type. The
            // long expression is then in the table definition once per
            // property, not once per index.
            let pcol = Self::property_column(p);
            parts.push(format!(
                "ADD COLUMN IF NOT EXISTS `{pcol}` VARCHAR({max}) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin \
                 AS ({}) VIRTUAL",
                Self::typed_text(p),
                max = crate::schema::MAX_INLINE
            ));
            // A text index holds strings only: the typed texts that aren't
            // a number (`n`) or a boolean (`b`).
            let val = match d.kind {
                IndexKind::Range => format!("`{pcol}`"),
                IndexKind::Text => format!("IF(LEFT(`{pcol}`, 1) IN ('n', 'b'), NULL, `{pcol}`)"),
            };
            let col = Self::index_column(d, k);
            parts.push(format!(
                "ADD COLUMN IF NOT EXISTS `{col}` VARCHAR({max}) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin \
                 AS (IF({cond}, {val}, NULL)) VIRTUAL",
                max = crate::schema::MAX_INLINE
            ));
            cols.push(format!("`{col}`"));
        }
        parts.push(format!(
            "ADD {}INDEX IF NOT EXISTS `_sprig_i{}` ({})",
            if d.unique { "UNIQUE " } else { "" },
            d.id,
            cols.join(", ")
        ));
        Ok(format!("ALTER TABLE {table} {}", parts.join(", ")))
    }

    /// `ALTER TABLE` dropping index `d` and its columns.
    #[must_use]
    pub fn index_drop_ddl(&self, d: &crate::store::IndexDef) -> String {
        let table = match d.entity {
            crate::ast::Entity::Node => &self.nodes,
            crate::ast::Entity::Relationship => &self.edges,
        };
        let mut parts = vec![format!("DROP INDEX IF EXISTS `_sprig_i{}`", d.id)];
        for k in 0..d.props.len() {
            parts.push(format!(
                "DROP COLUMN IF EXISTS `{}`",
                Self::index_column(d, k)
            ));
        }
        format!("ALTER TABLE {table} {}", parts.join(", "))
    }

    /// `DROP TABLE IF EXISTS` for all five.
    #[must_use]
    pub fn drop_ddl(&self) -> String {
        format!(
            "DROP TABLE IF EXISTS {}, {}, {}, {}, {}, {}",
            self.nodes, self.edges, self.labels, self.mirrors, self.seq, self.claims
        )
    }

    /// `DROP TABLE IF EXISTS` for one shard's four.
    #[must_use]
    pub fn shard_drop_ddl(&self) -> String {
        format!(
            "DROP TABLE IF EXISTS {}, {}, {}, {}, {}",
            self.nodes, self.edges, self.labels, self.mirrors, self.claims
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
    /// `CREATE [OR REPLACE] PROPERTY GRAPH ...` (a mapped graph).
    CreateProperty(Box<crate::mapped::CreatePropertyGraph>),
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
/// 39900 for malformed graph DDL; 39908 for a mapped graph this build
/// can't map; 39901 for `ALTER GRAPH`.
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
        | ["CREATE", "OR", "REPLACE", "PROPERTY", "GRAPH", ..] => {
            Ok(crate::mapped::parse_create(sql)?.map(|c| GraphDdl::CreateProperty(Box::new(c))))
        }
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
                .expect_err("no key")
                .code(),
            Some(SprigCode::GraphDefinitionInvalid)
        );
        assert!(matches!(
            parse_ddl("CREATE PROPERTY GRAPH shop VERTEX TABLES (x KEY (id))"),
            Ok(Some(GraphDdl::CreateProperty(_)))
        ));
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
        assert_eq!(t.create_ddl().len(), 7);
        assert_eq!(t.shard_ddl().len(), 5);
        assert!(t.mirrors.ends_with("`__sprig_gm_901`"));
    }
}
