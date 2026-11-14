//! Reading one physical table's statistics from MariaDB.
//!
//! Every query is filtered by schema and table and passes them as
//! parameters. A node that denies the `mysql.*` statistics tables (1142,
//! 1044) still yields column types, existing indexes and `SHOW INDEX`
//! cardinalities, with [`TableStats::eits`] false (selectivities then come
//! from `SHOW INDEX` and defaults).

use std::collections::BTreeMap;

use mysql_async::Row;
use mysql_async::prelude::Queryable;
use sprig_common::names::quote;
use sprig_common::{Result, SprigError};

use super::histogram::{Histogram, KeyClass};
use super::{ColumnInfo, ColumnStats, TableStats};
use crate::autoindex::model::{IndexInfo, KeyPart};
use crate::autoindex::store::origin_of;
use crate::sql::{col, errno, map_err};

/// Reads statistics; holds nothing but options.
#[derive(Debug, Clone, Copy)]
pub struct StatsReader {
    /// Replace text histogram bounds by `WEIGHT_STRING()` under each
    /// column's collation, so comparisons follow it.
    pub weigh_text: bool,
}

impl Default for StatsReader {
    fn default() -> Self {
        Self { weigh_text: true }
    }
}

/// Classifies `DATA_TYPE`: the key class, whether it is a large object,
/// and the bytes of a fixed-width type.
#[must_use]
pub fn classify(data_type: &str) -> (Option<KeyClass>, bool, Option<u64>) {
    match data_type {
        "tinyint" => (Some(KeyClass::Numeric), false, Some(1)),
        "smallint" | "year" => (Some(KeyClass::Numeric), false, Some(2)),
        "mediumint" | "time" => (
            Some(if data_type == "time" {
                KeyClass::Temporal
            } else {
                KeyClass::Numeric
            }),
            false,
            Some(3),
        ),
        "int" | "integer" | "float" | "timestamp" => (
            Some(if data_type == "timestamp" {
                KeyClass::Temporal
            } else {
                KeyClass::Numeric
            }),
            false,
            Some(4),
        ),
        "bigint" | "double" | "real" => (Some(KeyClass::Numeric), false, Some(8)),
        "decimal" | "numeric" | "bit" => (Some(KeyClass::Numeric), false, None),
        "date" => (Some(KeyClass::Temporal), false, Some(3)),
        "datetime" => (Some(KeyClass::Temporal), false, Some(5)),
        "char" | "varchar" | "enum" | "set" | "binary" | "varbinary" | "inet4" | "inet6"
        | "uuid" => (Some(KeyClass::Text), false, None),
        "tinytext" | "text" | "mediumtext" | "longtext" | "tinyblob" | "blob" | "mediumblob"
        | "longblob" | "json" | "geometry" | "point" | "linestring" | "polygon" | "multipoint"
        | "multilinestring" | "multipolygon" | "geometrycollection" => (None, true, None),
        _ => (None, false, None),
    }
}

fn denied(e: &SprigError) -> bool {
    matches!(errno(e), Some(1142 | 1044 | 1143 | 1227))
}

impl StatsReader {
    /// Loads `schema`.`table`'s statistics.
    ///
    /// # Errors
    /// 39070 or a backend error from `information_schema` (the statistics
    /// tables' access errors are absorbed); `Internal` for a malformed row.
    pub async fn read<Q: Queryable + Send>(
        &self,
        q: &mut Q,
        schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        let mut t = TableStats {
            schema: schema.to_owned(),
            table: table.to_owned(),
            ..TableStats::default()
        };
        read_columns(q, &mut t).await?;
        read_indexes(q, &mut t).await?;
        read_size(q, &mut t).await?;
        match read_eits(q, &mut t).await {
            Ok(()) => t.eits = true,
            Err(e) if denied(&e) => {
                tracing::warn!(schema, table, "no access to mysql statistics tables");
                t.eits = false;
            }
            Err(e) => return Err(e),
        }
        if self.weigh_text {
            reweigh(q, &mut t).await?;
        }
        Ok(t)
    }
}

async fn read_columns<Q: Queryable + Send>(q: &mut Q, t: &mut TableStats) -> Result<()> {
    let rows: Vec<Row> = q
        .exec(
            "SELECT LOWER(COLUMN_NAME), LOWER(DATA_TYPE), CHARACTER_SET_NAME, COLLATION_NAME, \
             IS_NULLABLE, CHARACTER_OCTET_LENGTH, COLUMN_KEY, GENERATION_EXPRESSION \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (&t.schema, &t.table),
        )
        .await
        .map_err(map_err)?;
    for mut r in rows {
        let name: String = col(&mut r, 0, "COLUMN_NAME")?;
        let data_type: String = col(&mut r, 1, "DATA_TYPE")?;
        let (class, lob, fixed) = classify(&data_type);
        let octets: Option<u64> = col(&mut r, 5, "CHARACTER_OCTET_LENGTH")?;
        let key: String = col(&mut r, 6, "COLUMN_KEY")?;
        let generated: Option<String> = col(&mut r, 7, "GENERATION_EXPRESSION")?;
        if key == "PRI" {
            // Ordered below from STATISTICS; this keeps tables whose
            // STATISTICS row is filtered.
            t.primary_key.push(name.clone());
        }
        t.columns.insert(
            name.clone(),
            ColumnInfo {
                name,
                class,
                charset: col(&mut r, 2, "CHARACTER_SET_NAME")?,
                collation: col(&mut r, 3, "COLLATION_NAME")?,
                nullable: col::<String>(&mut r, 4, "IS_NULLABLE")? == "YES",
                max_bytes: fixed.or(octets),
                lob,
                generated: generated.is_some_and(|g| !g.is_empty()),
                data_type,
            },
        );
    }
    Ok(())
}

async fn read_indexes<Q: Queryable + Send>(q: &mut Q, t: &mut TableStats) -> Result<()> {
    let rows: Vec<Row> = q
        .exec(
            "SELECT INDEX_NAME, SEQ_IN_INDEX, LOWER(COLUMN_NAME), COLLATION, CARDINALITY, \
             SUB_PART, NON_UNIQUE, IGNORED FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (&t.schema, &t.table),
        )
        .await
        .map_err(map_err)?;
    let mut by_name: BTreeMap<String, IndexInfo> = BTreeMap::new();
    for mut r in rows {
        let name: String = col(&mut r, 0, "INDEX_NAME")?;
        let column: Option<String> = col(&mut r, 2, "COLUMN_NAME")?;
        let collation: Option<String> = col(&mut r, 3, "COLLATION")?;
        let card: Option<u64> = col(&mut r, 4, "CARDINALITY")?;
        let sub: Option<u64> = col(&mut r, 5, "SUB_PART")?;
        let non_unique: i64 = col(&mut r, 6, "NON_UNIQUE")?;
        let ignored: String = col(&mut r, 7, "IGNORED")?;
        t.show_index.entry(name.clone()).or_default().push(card);
        let e = by_name.entry(name.clone()).or_insert_with(|| IndexInfo {
            origin: origin_of(&name, None),
            name: name.clone(),
            parts: Vec::new(),
            unique: non_unique == 0,
            ignored: ignored == "YES",
            usable: true,
        });
        e.parts.push(KeyPart {
            // An expression key part has no column.
            column: column.unwrap_or_default(),
            prefix: sub.and_then(|s| u16::try_from(s).ok()),
            desc: collation.as_deref() == Some("D"),
        });
    }
    if let Some(pk) = by_name.get("PRIMARY") {
        t.primary_key = pk.parts.iter().map(|p| p.column.clone()).collect();
    }
    // PRIMARY first, then by name.
    let mut list: Vec<IndexInfo> = by_name.into_values().collect();
    list.sort_by_key(|i| (i.name != "PRIMARY", i.name.clone()));
    t.indexes = list;
    Ok(())
}

async fn read_size<Q: Queryable + Send>(q: &mut Q, t: &mut TableStats) -> Result<()> {
    let row: Option<(Option<u64>, Option<u64>, Option<u64>)> = q
        .exec_first(
            "SELECT DATA_LENGTH, INDEX_LENGTH, TABLE_ROWS FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (&t.schema, &t.table),
        )
        .await
        .map_err(map_err)?;
    if let Some((data, index, rows)) = row {
        t.data_bytes = data.unwrap_or(0);
        t.index_bytes = index.unwrap_or(0);
        t.n_rows = rows;
    }
    Ok(())
}

async fn read_eits<Q: Queryable + Send>(q: &mut Q, t: &mut TableStats) -> Result<()> {
    let card: Option<Option<u64>> = q
        .exec_first(
            "SELECT cardinality FROM mysql.table_stats WHERE db_name = ? AND table_name = ?",
            (&t.schema, &t.table),
        )
        .await
        .map_err(map_err)?;
    t.cardinality = card.flatten();
    let n_rows: Option<u64> = q
        .exec_first(
            "SELECT n_rows FROM mysql.innodb_table_stats WHERE database_name = ? \
             AND table_name = ?",
            (&t.schema, &t.table),
        )
        .await
        .map_err(map_err)?;
    if n_rows.is_some() {
        t.n_rows = n_rows;
    }
    let rows: Vec<Row> = q
        .exec(
            "SELECT LOWER(column_name), nulls_ratio, avg_length, avg_frequency, hist_type, \
             histogram FROM mysql.column_stats WHERE db_name = ? AND table_name = ?",
            (&t.schema, &t.table),
        )
        .await
        .map_err(map_err)?;
    for mut r in rows {
        let name: String = col(&mut r, 0, "column_name")?;
        let num = |r: &mut Row, i: usize, n: &str| -> Result<f64> {
            let v: Option<String> = col(r, i, n)?;
            Ok(v.and_then(|s| s.parse().ok()).unwrap_or(0.0))
        };
        let nulls_ratio = num(&mut r, 1, "nulls_ratio")?;
        let avg_length = num(&mut r, 2, "avg_length")?;
        let avg_frequency = num(&mut r, 3, "avg_frequency")?;
        let hist_type: Option<String> = col(&mut r, 4, "hist_type")?;
        let raw: Option<Vec<u8>> = col(&mut r, 5, "histogram")?;
        let class = t.columns.get(&name).and_then(|c| c.class);
        let histogram = match (hist_type.as_deref(), raw, class) {
            (Some("JSON_HB"), Some(raw), Some(class)) => match Histogram::parse(class, &raw) {
                Ok(h) => Some(h),
                Err(e) => {
                    tracing::warn!(column = %name, error = %e, "unreadable JSON_HB histogram");
                    None
                }
            },
            // SINGLE_PREC_HB and DOUBLE_PREC_HB (10.x) are not read: their
            // bounds are scaled between min_value and max_value and need
            // the column's type to decode; ANALYZE on 11.x writes JSON_HB.
            _ => None,
        };
        t.column_stats.insert(
            name,
            ColumnStats {
                nulls_ratio,
                avg_length,
                avg_frequency,
                histogram,
            },
        );
    }
    let rows: Vec<(String, u32, Option<String>)> = q
        .exec(
            "SELECT index_name, prefix_arity, avg_frequency FROM mysql.index_stats \
             WHERE db_name = ? AND table_name = ? ORDER BY index_name, prefix_arity",
            (&t.schema, &t.table),
        )
        .await
        .map_err(map_err)?;
    for (index, arity, freq) in rows {
        let v = t.index_stats.entry(index).or_default();
        let at = usize::try_from(arity.saturating_sub(1)).unwrap_or(0);
        if v.len() <= at {
            v.resize(at + 1, 0.0);
        }
        v[at] = freq.and_then(|s| s.parse().ok()).unwrap_or(0.0);
    }
    Ok(())
}

/// Replaces every text histogram's bounds with weight strings.
async fn reweigh<Q: Queryable + Send>(q: &mut Q, t: &mut TableStats) -> Result<()> {
    let names: Vec<String> = t.column_stats.keys().cloned().collect();
    for name in names {
        let Some(info) = t.columns.get(&name) else {
            continue;
        };
        let (Some(cs), Some(coll)) = (info.charset.clone(), info.collation.clone()) else {
            continue;
        };
        let Some(h) = t
            .column_stats
            .get(&name)
            .and_then(|s| s.histogram.as_ref())
            .filter(|h| h.class == KeyClass::Text)
        else {
            continue;
        };
        let texts: Vec<String> = h.bound_texts().into_iter().map(str::to_owned).collect();
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        let w = weigh(q, &cs, &coll, &refs).await?;
        if let Some(h) = t
            .column_stats
            .get_mut(&name)
            .and_then(|s| s.histogram.as_mut())
        {
            h.reweigh(w);
        }
    }
    Ok(())
}

/// `WEIGHT_STRING()` of each of `values` converted to `charset` under
/// `collation`, so text compares as the column does. Values that don't
/// convert get their raw bytes.
///
/// # Errors
/// `Internal` for a charset or collation name that isn't an identifier;
/// 39070 or a backend error.
pub async fn weigh<Q: Queryable + Send>(
    q: &mut Q,
    charset: &str,
    collation: &str,
    values: &[&str],
) -> Result<Vec<Vec<u8>>> {
    let ident =
        |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if !ident(charset) || !ident(collation) {
        return Err(SprigError::internal(format!(
            "bad charset or collation {charset:?} {collation:?}"
        )));
    }
    let mut out = Vec::with_capacity(values.len());
    for chunk in values.chunks(256) {
        let expr = format!(
            "WEIGHT_STRING(CONVERT(? USING {cs}) COLLATE {co})",
            cs = quote(charset),
            co = quote(collation)
        );
        let sql = format!("SELECT {}", vec![expr.as_str(); chunk.len()].join(", "));
        let row: Option<Row> = q.exec_first(sql, chunk.to_vec()).await.map_err(map_err)?;
        let mut row = row.ok_or_else(|| SprigError::internal("WEIGHT_STRING returned nothing"))?;
        for (i, v) in chunk.iter().enumerate() {
            let w: Option<Vec<u8>> = col(&mut row, i, "WEIGHT_STRING")?;
            out.push(w.unwrap_or_else(|| v.as_bytes().to_vec()));
        }
    }
    Ok(out)
}
