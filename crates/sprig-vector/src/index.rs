//! Standalone vector indexes:
//! one table per index in a logical database, its definition in the
//! table's comment, so the index needs no catalog row.
//!
//! ```sql
//! CREATE TABLE `<name>` (
//!   id     VARBINARY(254) NOT NULL PRIMARY KEY,
//!   v      VECTOR(<dimensions>) NOT NULL,
//!   doc    JSON NULL,             -- metadata, stored as the document store stores documents
//!   _types JSON NULL,             -- its type map
//!   [lat DOUBLE NULL, lng DOUBLE NULL, _sprig_s2 BIGINT UNSIGNED NULL, KEY (_sprig_s2)]
//!   [VECTOR INDEX ann (v) M=<m> DISTANCE=<metric>]   -- once the ANN graph is built
//! ) COMMENT='sprig:vector {"dimensions":..,"similarity":..,"m":..,"ann_mode":..,"ann_state":..}'
//! ```

use serde_json::{Value, json};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_kv::SqlRunner;
use sprig_kv::sql::{ident, lit};

use crate::backend;

/// The comment's prefix.
pub const COMMENT_PREFIX: &str = "sprig:vector ";
/// The ANN index's name inside the table.
pub const ANN_INDEX: &str = "ann";
/// Largest number of dimensions (MariaDB's `VECTOR`).
pub const MAX_DIMENSIONS: u32 = 16383;

/// The distance MariaDB computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// `VEC_DISTANCE_EUCLIDEAN`.
    Euclidean,
    /// `VEC_DISTANCE_COSINE`: 1 − cosine similarity.
    Cosine,
}

impl Metric {
    /// The SQL distance function.
    #[must_use]
    pub const fn function(self) -> &'static str {
        match self {
            Self::Euclidean => "VEC_DISTANCE_EUCLIDEAN",
            Self::Cosine => "VEC_DISTANCE_COSINE",
        }
    }

    /// `DISTANCE=` of the index.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Euclidean => "euclidean",
            Self::Cosine => "cosine",
        }
    }
}

/// What the client asked for: a metric, or `dotProduct` (unit vectors
/// under a cosine index, as MongoDB Atlas).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Similarity {
    /// Euclidean distance.
    Euclidean,
    /// Cosine.
    Cosine,
    /// Dot product of unit vectors: stored normalized, searched as cosine.
    DotProduct,
}

impl Similarity {
    /// Parses `euclidean`, `cosine` or `dotProduct` (any case).
    ///
    /// # Errors
    /// 39737 for anything else.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "" | "euclidean" | "l2" => Ok(Self::Euclidean),
            "cosine" => Ok(Self::Cosine),
            "dotproduct" | "dot_product" => Ok(Self::DotProduct),
            other => Err(SprigError::new(
                SprigCode::VectorMetricMismatch,
                format!("unknown similarity '{other}': use euclidean, cosine or dotProduct"),
            )),
        }
    }

    /// Its name, as MongoDB spells it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Euclidean => "euclidean",
            Self::Cosine => "cosine",
            Self::DotProduct => "dotProduct",
        }
    }

    /// The metric MariaDB computes.
    #[must_use]
    pub const fn metric(self) -> Metric {
        match self {
            Self::Euclidean => Metric::Euclidean,
            Self::Cosine | Self::DotProduct => Metric::Cosine,
        }
    }

    /// The score from a distance: Euclidean `1 / (1 + d)`;
    /// cosine and `dotProduct` `(1 + similarity) / 2`, which is `1 - d/2`.
    #[must_use]
    pub fn score(self, d: f64) -> f64 {
        match self {
            Self::Euclidean => 1.0 / (1.0 + d),
            Self::Cosine | Self::DotProduct => 1.0 - d / 2.0,
        }
    }
}

/// When the HNSW graph exists (`vector_indexes.ann_mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnMode {
    /// Built once a shard holds `vector.ann_min_rows` rows and validated
    /// by recall.
    Auto,
    /// Built at once.
    On,
    /// Never: searches are exact.
    Off,
}

impl AnnMode {
    /// Parses `auto`, `on` or `off` (empty: `auto`).
    ///
    /// # Errors
    /// 39792 for anything else.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Self::Auto),
            "on" => Ok(Self::On),
            "off" => Ok(Self::Off),
            other => Err(SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("ann_mode '{other}': use auto, on or off"),
            )),
        }
    }

    /// Its name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::On => "on",
            Self::Off => "off",
        }
    }
}

/// Whether the graph is there (`vector_indexes.ann_state`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnState {
    /// No graph: searches are exact.
    None,
    /// Being built: searches stay exact.
    Building,
    /// Built and accepted.
    Ready,
}

impl AnnState {
    fn parse(s: &str) -> Self {
        match s {
            "building" => Self::Building,
            "ready" => Self::Ready,
            _ => Self::None,
        }
    }

    /// Its name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Building => "building",
            Self::Ready => "ready",
        }
    }
}

/// One standalone index.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexDef {
    /// The logical database.
    pub database: String,
    /// The index (its table's name).
    pub name: String,
    /// Vector length.
    pub dimensions: u32,
    /// The similarity.
    pub similarity: Similarity,
    /// HNSW's M.
    pub m: u8,
    /// When the graph is built.
    pub ann_mode: AnnMode,
    /// Whether it is.
    pub ann_state: AnnState,
    /// Items carry a location: `lat`, `lng` and `_sprig_s2`.
    pub geo: bool,
    /// The last recall measured when the graph was validated.
    pub recall: Option<f64>,
}

/// Validates an index or database name: 1 to 64 letters, digits or
/// underscores, not starting with `__sprig`.
///
/// # Errors
/// 39792.
pub fn check_name(what: &str, n: &str) -> Result<()> {
    if n.is_empty()
        || n.len() > 64
        || n.starts_with("__sprig")
        || !n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("'{n}' can't be a {what}: names are 1 to 64 letters, digits or underscores"),
        ));
    }
    Ok(())
}

impl IndexDef {
    /// A new index's definition, checked.
    ///
    /// # Errors
    /// 39792 for names, 39736 for dimensions, 39737 for the similarity.
    pub fn new(
        database: &str,
        name: &str,
        dimensions: u32,
        similarity: Similarity,
        m: u8,
        ann_mode: AnnMode,
        geo: bool,
    ) -> Result<Self> {
        check_name("database", database)?;
        check_name("vector index", name)?;
        if dimensions == 0 || dimensions > MAX_DIMENSIONS {
            return Err(SprigError::new(
                SprigCode::VectorDimensionMismatch,
                format!("dimensions must be 1 to {MAX_DIMENSIONS}, not {dimensions}"),
            ));
        }
        Ok(Self {
            database: database.to_owned(),
            name: name.to_owned(),
            dimensions,
            similarity,
            m: m.clamp(3, 200),
            ann_mode,
            ann_state: AnnState::None,
            geo,
            recall: None,
        })
    }

    /// The table, qualified.
    #[must_use]
    pub fn table(&self) -> String {
        format!("{}.{}", ident(&self.database), ident(&self.name))
    }

    /// The comment holding the definition.
    #[must_use]
    pub fn comment(&self) -> String {
        let mut j = json!({
            "dimensions": self.dimensions,
            "similarity": self.similarity.name(),
            "m": self.m,
            "ann_mode": self.ann_mode.name(),
            "ann_state": self.ann_state.name(),
        });
        if self.geo {
            j["geo"] = json!(true);
        }
        if let Some(r) = self.recall {
            j["recall"] = json!((r * 1000.0).round() / 1000.0);
        }
        format!("{COMMENT_PREFIX}{j}")
    }

    /// The definition from a table comment.
    #[must_use]
    pub fn from_comment(database: &str, name: &str, comment: &str) -> Option<Self> {
        let body = comment.strip_prefix(COMMENT_PREFIX)?;
        let j: Value = serde_json::from_str(body).ok()?;
        Some(Self {
            database: database.to_owned(),
            name: name.to_owned(),
            dimensions: u32::try_from(j["dimensions"].as_u64()?).ok()?,
            similarity: Similarity::parse(j["similarity"].as_str().unwrap_or("euclidean")).ok()?,
            m: u8::try_from(j["m"].as_u64().unwrap_or(16)).unwrap_or(16),
            ann_mode: AnnMode::parse(j["ann_mode"].as_str().unwrap_or("auto")).ok()?,
            ann_state: AnnState::parse(j["ann_state"].as_str().unwrap_or("none")),
            geo: j["geo"].as_bool().unwrap_or(false),
            recall: j["recall"].as_f64(),
        })
    }

    /// The `VECTOR INDEX` clause.
    #[must_use]
    pub fn ann_clause(&self) -> String {
        format!(
            "VECTOR INDEX {ANN_INDEX} (v) M={} DISTANCE={}",
            self.m,
            self.similarity.metric().name()
        )
    }

    /// `CREATE TABLE` of the index; with the graph when `ann_mode` is `on`.
    #[must_use]
    pub fn create_sql(&self) -> String {
        let mut cols = vec![
            "id VARBINARY(254) NOT NULL".to_owned(),
            format!("v VECTOR({}) NOT NULL", self.dimensions),
            "doc JSON NULL".to_owned(),
            "_types JSON NULL".to_owned(),
        ];
        if self.geo {
            cols.push("lat DOUBLE NULL".into());
            cols.push("lng DOUBLE NULL".into());
            cols.push("_sprig_s2 BIGINT UNSIGNED NULL".into());
        }
        cols.push("PRIMARY KEY (id)".into());
        if self.geo {
            cols.push("KEY s2 (_sprig_s2)".into());
        }
        if self.ann_state == AnnState::Ready {
            cols.push(self.ann_clause());
        }
        format!(
            "CREATE TABLE {} ({}) COMMENT={}",
            self.table(),
            cols.join(", "),
            lit_text(&self.comment())
        )
    }
}

/// A text literal (`'...'`), quotes and backslashes escaped.
#[must_use]
pub fn lit_text(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// Reads an index's definition.
///
/// # Errors
/// 39735 when there is no such index; the backend's.
pub async fn describe(sql: &mut dyn SqlRunner, database: &str, name: &str) -> Result<IndexDef> {
    check_name("database", database)?;
    check_name("vector index", name)?;
    let rows = sql
        .query(&format!(
            "SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
            lit_text(database),
            lit_text(name)
        ))
        .await
        .map_err(backend)?;
    let comment = rows
        .first()
        .and_then(|r| r.first())
        .and_then(Clone::clone)
        .map(|c| String::from_utf8_lossy(&c).into_owned());
    comment
        .and_then(|c| IndexDef::from_comment(database, name, &c))
        .ok_or_else(|| {
            SprigError::new(
                SprigCode::VectorIndexNotFound,
                format!("no vector index '{database}.{name}'"),
            )
        })
}

/// Creates an index (and its database when missing).
///
/// # Errors
/// 1050 when the name is taken; the backend's.
pub async fn create(sql: &mut dyn SqlRunner, mut def: IndexDef) -> Result<IndexDef> {
    if def.ann_mode == AnnMode::On {
        def.ann_state = AnnState::Ready;
    }
    sql.exec(&format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        ident(&def.database)
    ))
    .await
    .map_err(backend)?;
    sql.exec(&def.create_sql()).await.map_err(backend)?;
    Ok(def)
}

/// Drops an index.
///
/// # Errors
/// 39735 when there is no such index; the backend's.
pub async fn drop(sql: &mut dyn SqlRunner, database: &str, name: &str) -> Result<()> {
    let def = describe(sql, database, name).await?;
    sql.exec(&format!("DROP TABLE {}", def.table()))
        .await
        .map_err(backend)?;
    Ok(())
}

/// Builds or drops the graph and records the new state in the comment, in
/// one `ALTER TABLE` (MariaDB 11.8 can't build it in place: the table is
/// copied).
///
/// # Errors
/// The backend's.
pub async fn set_ann(sql: &mut dyn SqlRunner, def: &mut IndexDef, on: bool) -> Result<()> {
    let had = def.ann_state == AnnState::Ready;
    def.ann_state = if on { AnnState::Ready } else { AnnState::None };
    let change = match (had, on) {
        (false, true) => format!("ADD {}, ", def.ann_clause()),
        (true, false) => format!("DROP INDEX {ANN_INDEX}, "),
        _ => String::new(),
    };
    sql.exec(&format!(
        "ALTER TABLE {} {change}COMMENT={}",
        def.table(),
        lit_text(&def.comment())
    ))
    .await
    .map_err(backend)?;
    Ok(())
}

/// Rows in the index.
///
/// # Errors
/// The backend's.
pub async fn rows(sql: &mut dyn SqlRunner, def: &IndexDef) -> Result<u64> {
    let r = sql
        .query(&format!("SELECT COUNT(*) FROM {}", def.table()))
        .await
        .map_err(backend)?;
    Ok(sprig_kv::sql::cell_u64(
        r.first().and_then(|r| r.first()).and_then(Option::as_ref),
    ))
}

/// An id as a SQL literal.
#[must_use]
pub fn id_lit(id: &[u8]) -> String {
    lit(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comments_round_trip() {
        let mut d = IndexDef::new(
            "shop",
            "emb",
            768,
            Similarity::DotProduct,
            16,
            AnnMode::Auto,
            true,
        )
        .unwrap();
        d.recall = Some(0.97321);
        let c = d.comment();
        assert!(c.starts_with(COMMENT_PREFIX));
        let back = IndexDef::from_comment("shop", "emb", &c).unwrap();
        assert_eq!(back.recall, Some(0.973));
        assert_eq!(back.similarity, Similarity::DotProduct);
        assert!(back.geo);
        assert!(IndexDef::from_comment("shop", "emb", "a plain table").is_none());
        assert!(d.create_sql().contains("v VECTOR(768) NOT NULL"));
        assert!(!d.create_sql().contains("VECTOR INDEX"));
        d.ann_state = AnnState::Ready;
        assert!(
            d.create_sql()
                .contains("VECTOR INDEX ann (v) M=16 DISTANCE=cosine")
        );
        assert!(
            IndexDef::new(
                "shop",
                "__sprig_x",
                3,
                Similarity::Cosine,
                16,
                AnnMode::Off,
                false
            )
            .is_err()
        );
        assert!(
            IndexDef::new("shop", "x", 0, Similarity::Cosine, 16, AnnMode::Off, false).is_err()
        );
    }

    #[test]
    fn scores_follow_atlas() {
        assert!((Similarity::Euclidean.score(1.0) - 0.5).abs() < 1e-12);
        assert!((Similarity::Cosine.score(0.0) - 1.0).abs() < 1e-12);
        assert!((Similarity::DotProduct.score(2.0)).abs() < 1e-12);
    }
}
