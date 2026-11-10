//! `sprigctl graph list|show|partition` (chapter 43 build step 30, R8
//! round 4): a tenant's graphs as the operator sees them, read from the
//! tenant catalog (`graphs`, `objects`, the label and type rows, the
//! indexes) and from the graph's shards.
//!
//! - `list`: every graph with its database, kind, table group and shard
//!   count.
//! - `show`: one graph: labels, relationship types, indexes and
//!   constraints, and each shard's cluster with its node, relationship,
//!   mirror and uniqueness-claim row counts.
//! - `partition`: how the graph is cut today (`part` = `id`, hash
//!   placement) and the share of relationships whose two ends live on
//!   different shards, from the edge tables' `dst_rk` against the map.
//!   The partitioning advisor of build step 25 (property scoring, label
//!   propagation) isn't built; this is its measurement.

use mysql_async::prelude::Queryable as _;
use serde::{Deserialize, Serialize};
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_shard::shard_schema;

use super::ShardingLoop;

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::BackendUnavailable, format!("{what}: {e}"))
}

/// One graph in `sprigctl graph list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphRow {
    /// `objects.object_id`.
    pub graph_id: u64,
    /// The logical database.
    pub database: String,
    /// The graph's name.
    pub name: String,
    /// `native` or `mapped`.
    pub kind: String,
    /// `active`, ...
    pub state: String,
    /// The table group of a native graph from round 3 on.
    pub group: Option<u32>,
    /// Shards (1 for graphs without a group).
    pub shards: usize,
}

/// One shard of a graph in `sprigctl graph show`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphShardRow {
    /// The shard id (1 for a graph without a group).
    pub shard: u32,
    /// Its cluster.
    pub cluster: String,
    /// Its schema.
    pub schema: String,
    /// Node rows.
    pub nodes: u64,
    /// Relationship rows (on their source's shard).
    pub relationships: u64,
    /// Mirror rows (on their destination's shard).
    pub mirrors: u64,
    /// Uniqueness claims routed here.
    pub claims: u64,
}

/// An index or constraint in `sprigctl graph show`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphIndexRow {
    /// The name.
    pub name: String,
    /// `range` or `text`.
    pub kind: String,
    /// A uniqueness constraint.
    pub unique: bool,
    /// `node` or `relationship`.
    pub entity: String,
    /// The label or type.
    pub token: String,
    /// The properties.
    pub properties: Vec<String>,
    /// `building` or `online`.
    pub state: String,
}

/// `sprigctl graph show`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphView {
    /// The graph.
    pub graph: GraphRow,
    /// Labels.
    pub labels: Vec<String>,
    /// Relationship types.
    pub types: Vec<String>,
    /// Indexes and constraints.
    pub indexes: Vec<GraphIndexRow>,
    /// Shards with their row counts (native graphs).
    pub shards: Vec<GraphShardRow>,
}

/// `sprigctl graph partition`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphPartition {
    /// The graph.
    pub graph: GraphRow,
    /// How nodes are placed: `id` (hash of the node id) today.
    pub partition_by: String,
    /// Relationships counted.
    pub relationships: u64,
    /// Of which with their ends on different shards.
    pub cross_shard: u64,
    /// `cross_shard` / `relationships`, in percent.
    pub cross_shard_pct: f64,
    /// What the numbers suggest.
    pub advice: String,
}

/// `graphs` joined with `objects` and `databases`: id, database id and
/// name, graph name, kind, state, group, the database's default group.
type GraphCatalogRow = (u64, u32, String, String, String, String, Option<u32>, u32);

/// A graph's catalog row and where its shards are.
struct Located {
    row: GraphRow,
    /// (shard, cluster, schema, first hash).
    shards: Vec<(u32, ClusterId, String, u64)>,
}

impl ShardingLoop {
    /// The tenant's home cluster (its catalog and default groups).
    async fn graph_home(&self, tenant: TenantId) -> Result<ClusterId> {
        let (tm, _) = self.tenant_maps.load(tenant).await?;
        Ok(sprig_shard::Placement::of(&tm)?.cluster)
    }

    async fn graph_rows(&self, tenant: TenantId) -> Result<Vec<Located>> {
        let home = self.graph_home(tenant).await?;
        let mut c = self.conn(&home).await?;
        let cat = SchemaName::catalog(tenant).to_string();
        let have: Option<String> = c
            .exec_first(
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? \
                 AND TABLE_NAME = 'graphs'",
                (cat.clone(),),
            )
            .await
            .map_err(|e| db_err("graph catalog", &e))?;
        if have.is_none() {
            return Ok(Vec::new());
        }
        let rows: Vec<GraphCatalogRow> = c
            .query(format!(
                "SELECT g.graph_id, g.db_id, d.name, o.name, g.kind, g.state, g.group_id, \
                 d.default_group_id \
                 FROM {q}.graphs g JOIN {q}.objects o ON o.object_id = g.graph_id \
                 JOIN {q}.`databases` d ON d.db_id = g.db_id \
                 WHERE o.state <> 'dropped' ORDER BY d.name, o.name",
                q = quote(&cat)
            ))
            .await
            .map_err(|e| db_err("graphs", &e))?;
        let mut out = Vec::new();
        for (id, db, dbname, name, kind, state, group, default_group) in rows {
            let shards = match group {
                Some(g) => match self.maps.load(tenant, GroupId(g)).await {
                    Ok((m, _)) => m
                        .ranges
                        .iter()
                        .map(|r| {
                            (
                                r.shard.0,
                                r.cluster.clone(),
                                shard_schema(tenant, DbId(db), GroupId(g), r.shard),
                                r.lo,
                            )
                        })
                        .collect(),
                    Err(_) => Vec::new(),
                },
                None => vec![(
                    1,
                    home.clone(),
                    shard_schema(tenant, DbId(db), GroupId(default_group), ShardId(1)),
                    0,
                )],
            };
            out.push(Located {
                row: GraphRow {
                    graph_id: id,
                    database: dbname,
                    name,
                    kind,
                    state,
                    group,
                    shards: shards.len(),
                },
                shards,
            });
        }
        Ok(out)
    }

    async fn find_graph(&self, tenant: TenantId, graph: &str) -> Result<Located> {
        let (db, name) = graph.split_once('.').ok_or_else(|| {
            SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("{graph}: name the graph as database.graph"),
            )
        })?;
        self.graph_rows(tenant)
            .await?
            .into_iter()
            .find(|g| g.row.database == db && g.row.name == name)
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::GraphNotFound,
                    format!("Graph '{graph}' doesn't exist"),
                )
            })
    }

    /// `sprigctl graph list <tenant>`.
    ///
    /// # Errors
    /// Store and catalog errors.
    pub async fn graphs(&self, tenant: TenantId) -> Result<Vec<GraphRow>> {
        Ok(self
            .graph_rows(tenant)
            .await?
            .into_iter()
            .map(|g| g.row)
            .collect())
    }

    async fn count(&self, cluster: &ClusterId, schema: &str, table: &str) -> u64 {
        let Ok(mut c) = self.conn(cluster).await else {
            return 0;
        };
        c.query_first::<u64, _>(format!(
            "SELECT COUNT(*) FROM {}.{}",
            quote(schema),
            quote(table)
        ))
        .await
        .ok()
        .flatten()
        .unwrap_or(0)
    }

    /// `sprigctl graph show <tenant> <db.graph>`.
    ///
    /// # Errors
    /// 39902 for an unknown graph; store and catalog errors.
    pub async fn graph_show(&self, tenant: TenantId, graph: &str) -> Result<GraphView> {
        let g = self.find_graph(tenant, graph).await?;
        let home = self.graph_home(tenant).await?;
        let mut c = self.conn(&home).await?;
        let q = quote(&SchemaName::catalog(tenant).to_string());
        let id = g.row.graph_id;
        let labels: Vec<String> = c
            .query(format!(
                "SELECT label FROM {q}.graph_node_sources WHERE graph_id = {id} ORDER BY label"
            ))
            .await
            .map_err(|e| db_err("labels", &e))?;
        let types: Vec<String> = c
            .query(format!(
                "SELECT type FROM {q}.graph_edge_sources WHERE graph_id = {id} ORDER BY type"
            ))
            .await
            .map_err(|e| db_err("types", &e))?;
        let idx: Vec<(String, String, bool, String, String, String, String)> = c
            .query(format!(
                "SELECT i.name, i.kind, i.is_unique, i.entity, \
                 COALESCE(IF(i.entity = 'node', n.label, e.type), CAST(i.source_id AS CHAR)), \
                 CAST(i.properties AS CHAR), i.state \
                 FROM {q}.graph_indexes i \
                 LEFT JOIN {q}.graph_node_sources n ON n.graph_id = i.graph_id AND n.source_id = i.source_id \
                 LEFT JOIN {q}.graph_edge_sources e ON e.graph_id = i.graph_id AND e.source_id = i.source_id \
                 WHERE i.graph_id = {id} ORDER BY i.name"
            ))
            .await
            .unwrap_or_default();
        drop(c);
        let indexes = idx
            .into_iter()
            .map(
                |(name, kind, unique, entity, token, props, state)| GraphIndexRow {
                    name,
                    kind,
                    unique,
                    entity,
                    token,
                    properties: serde_json::from_str(&props).unwrap_or_default(),
                    state,
                },
            )
            .collect();
        let mut shards = Vec::new();
        if g.row.kind == "native" {
            for (shard, cluster, schema, _) in &g.shards {
                shards.push(GraphShardRow {
                    shard: *shard,
                    cluster: cluster.to_string(),
                    schema: schema.clone(),
                    nodes: self
                        .count(cluster, schema, &format!("__sprig_gn_{id}"))
                        .await,
                    relationships: self
                        .count(cluster, schema, &format!("__sprig_ge_{id}"))
                        .await,
                    mirrors: self
                        .count(cluster, schema, &format!("__sprig_gm_{id}"))
                        .await,
                    claims: self
                        .count(cluster, schema, &format!("__sprig_gu_{id}"))
                        .await,
                });
            }
        }
        Ok(GraphView {
            graph: g.row,
            labels,
            types,
            indexes,
            shards,
        })
    }

    /// `sprigctl graph partition <tenant> <db.graph>`.
    ///
    /// # Errors
    /// 39902 for an unknown graph; store errors.
    pub async fn graph_partition(&self, tenant: TenantId, graph: &str) -> Result<GraphPartition> {
        let g = self.find_graph(tenant, graph).await?;
        if g.row.kind != "native" {
            return Err(SprigError::new(
                SprigCode::GraphReadOnlySource,
                format!("{graph} is a mapped graph: its tables' own sharding places it"),
            ));
        }
        let id = g.row.graph_id;
        let (mut total, mut cross) = (0u64, 0u64);
        for (i, (_, cluster, schema, lo)) in g.shards.iter().enumerate() {
            let hi = g.shards.get(i + 1).map(|s| s.3);
            let outside = match hi {
                Some(h) => format!("dst_rk < {lo} OR dst_rk >= {h}"),
                None => format!("dst_rk < {lo}"),
            };
            let mut c = self.conn(cluster).await?;
            let r: Option<(u64, u64)> = c
                .query_first(format!(
                    "SELECT COUNT(*), COALESCE(SUM({outside}), 0) FROM {}.{}",
                    quote(schema),
                    quote(&format!("__sprig_ge_{id}"))
                ))
                .await
                .map_err(|e| db_err("edges", &e))?;
            if let Some((t, x)) = r {
                total += t;
                cross += x;
            }
        }
        #[allow(clippy::cast_precision_loss)]
        let pct = if total == 0 {
            0.0
        } else {
            (cross as f64) * 100.0 / (total as f64)
        };
        let advice = if g.shards.len() < 2 {
            "one shard: every hop is local".to_owned()
        } else if pct > 50.0 {
            "most hops cross shards: a partition property or communities (graph.auto_partition, not built yet) would cut them".to_owned()
        } else {
            "hash placement keeps most hops local".to_owned()
        };
        Ok(GraphPartition {
            graph: g.row,
            partition_by: "id".into(),
            relationships: total,
            cross_shard: cross,
            cross_shard_pct: (pct * 10.0).round() / 10.0,
            advice,
        })
    }
}
