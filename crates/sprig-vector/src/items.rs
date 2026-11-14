//! Writes to a standalone index: dimensions are
//! checked before MariaDB sees a vector (39736), `dotProduct` indexes store
//! unit vectors (a zero vector is refused, 39737), and metadata is stored
//! as the document store stores documents (`doc` and its type map `_types`), so
//! filters use MongoDB's rules.

use std::fmt::Write as _;

use bson::{Bson, Document};
use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_doc::json::{RenderOpts, render, to_bson};
use sprig_kv::SqlRunner;

use crate::backend;
use crate::brute::normalized;
use crate::index::{IndexDef, Similarity, id_lit, lit_text};

/// Longest id.
pub const MAX_ID_BYTES: usize = 254;
/// Rows per `INSERT`.
const CHUNK: usize = 200;

/// One item to write.
#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    /// Its id.
    pub id: Vec<u8>,
    /// The vector.
    pub vector: Vec<f32>,
    /// Metadata: a JSON object (MongoDB Extended JSON accepted).
    pub metadata: Option<Value>,
    /// A location, `(latitude, longitude)` in degrees (geo indexes).
    pub location: Option<(f64, f64)>,
}

/// `VEC_FromText('[...]')` of a vector, each float in its shortest form
/// that reads back exactly.
///
/// # Errors
/// 39736 for NaN or an infinity.
pub fn vec_sql(v: &[f32]) -> Result<String> {
    let mut s = String::with_capacity(v.len() * 10 + 20);
    s.push_str("VEC_FromText('[");
    for (i, x) in v.iter().enumerate() {
        if !x.is_finite() {
            return Err(SprigError::new(
                SprigCode::VectorDimensionMismatch,
                "a vector holds NaN or an infinity",
            ));
        }
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "{x:?}");
    }
    s.push_str("]')");
    Ok(s)
}

/// Checks a vector's length (39736) and, for `dotProduct`, normalizes it
/// (a zero vector is 39737).
///
/// # Errors
/// See above.
pub fn prepare(def: &IndexDef, v: &[f32]) -> Result<Vec<f32>> {
    if v.len() != def.dimensions as usize {
        return Err(SprigError::new(
            SprigCode::VectorDimensionMismatch,
            format!(
                "the index '{}' holds {} dimensions; the vector has {}",
                def.name,
                def.dimensions,
                v.len()
            ),
        ));
    }
    if def.similarity == Similarity::DotProduct {
        return normalized(v).ok_or_else(|| {
            SprigError::new(
                SprigCode::VectorMetricMismatch,
                "a zero vector has no direction: dotProduct indexes refuse it",
            )
        });
    }
    Ok(v.to_vec())
}

/// Metadata as stored: `(doc, _types)`.
///
/// # Errors
/// 39724-like invalid argument (39792) for metadata that isn't an object.
pub fn stored_metadata(m: &Value) -> Result<(String, Option<String>)> {
    let bad = |e: String| SprigError::new(SprigCode::AdminRequestInvalid, e);
    let doc: Document = match Bson::try_from(m.clone()) {
        Ok(Bson::Document(d)) => d,
        Ok(_) => return Err(bad("metadata must be a JSON object".into())),
        Err(e) => return Err(bad(format!("metadata: {e}"))),
    };
    render(&doc, &RenderOpts::default()).map_err(|e| bad(format!("metadata: {}", e.message)))
}

/// Stored metadata back as a document.
#[must_use]
pub fn metadata_doc(doc: Option<&[u8]>, types: Option<&[u8]>) -> Document {
    let Some(d) = doc.and_then(|d| std::str::from_utf8(d).ok()) else {
        return Document::new();
    };
    let t = types.and_then(|t| std::str::from_utf8(t).ok());
    to_bson(d, t).unwrap_or_default()
}

/// Inserts or replaces items, `CHUNK` rows per statement.
///
/// # Errors
/// 39736, 39737 for vectors; 39792 for ids and metadata; the backend's.
pub async fn upsert(sql: &mut dyn SqlRunner, def: &IndexDef, items: &[Item]) -> Result<u64> {
    let mut rows = Vec::with_capacity(items.len());
    for it in items {
        if it.id.is_empty() || it.id.len() > MAX_ID_BYTES {
            return Err(SprigError::new(
                SprigCode::AdminRequestInvalid,
                format!("an id is 1 to {MAX_ID_BYTES} bytes"),
            ));
        }
        let v = prepare(def, &it.vector)?;
        let (doc, types) = match &it.metadata {
            Some(m) if !m.is_null() => {
                let (d, t) = stored_metadata(m)?;
                (
                    lit_text(&d),
                    t.map_or_else(|| "NULL".into(), |t| lit_text(&t)),
                )
            }
            _ => ("NULL".into(), "NULL".into()),
        };
        let mut row = format!("({}, {}, {doc}, {types}", id_lit(&it.id), vec_sql(&v)?);
        if def.geo {
            match it.location {
                Some((lat, lng)) => {
                    let cell = sprig_geo_cell(lat, lng)?;
                    let _ = write!(row, ", {lat:?}, {lng:?}, {cell}");
                }
                None => row.push_str(", NULL, NULL, NULL"),
            }
        }
        row.push(')');
        rows.push(row);
    }
    let cols = if def.geo {
        "(id, v, doc, _types, lat, lng, _sprig_s2)"
    } else {
        "(id, v, doc, _types)"
    };
    let update = if def.geo {
        "v = VALUES(v), doc = VALUES(doc), _types = VALUES(_types), lat = VALUES(lat), lng = VALUES(lng), _sprig_s2 = VALUES(_sprig_s2)"
    } else {
        "v = VALUES(v), doc = VALUES(doc), _types = VALUES(_types)"
    };
    let mut n = 0;
    for chunk in rows.chunks(CHUNK) {
        sql.exec(&format!(
            "INSERT INTO {} {cols} VALUES {} ON DUPLICATE KEY UPDATE {update}",
            def.table(),
            chunk.join(", ")
        ))
        .await
        .map_err(backend)?;
        n += chunk.len() as u64;
    }
    Ok(n)
}

/// The level-30 S2 cell of a location, checked.
///
/// # Errors
/// 39765 for coordinates out of range.
fn sprig_geo_cell(lat: f64, lng: f64) -> Result<u64> {
    if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lng) {
        return Err(SprigError::new(
            SprigCode::GeoInvalid,
            format!("latitude {lat} or longitude {lng} out of range"),
        ));
    }
    Ok(sprig_geo::s2::leaf(lat, lng))
}

/// Deletes items by id.
///
/// # Errors
/// The backend's.
pub async fn delete(sql: &mut dyn SqlRunner, def: &IndexDef, ids: &[Vec<u8>]) -> Result<u64> {
    let mut n = 0;
    for chunk in ids.chunks(CHUNK) {
        let list: Vec<String> = chunk.iter().map(|i| id_lit(i)).collect();
        n += sql
            .exec(&format!(
                "DELETE FROM {} WHERE id IN ({})",
                def.table(),
                list.join(", ")
            ))
            .await
            .map_err(backend)?;
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::AnnMode;

    #[test]
    fn vectors_and_metadata() {
        assert_eq!(
            vec_sql(&[1.0, 0.1, -2.5]).unwrap(),
            "VEC_FromText('[1.0,0.1,-2.5]')"
        );
        assert!(vec_sql(&[f32::NAN]).is_err());
        let d = IndexDef::new(
            "db",
            "i",
            2,
            Similarity::DotProduct,
            16,
            AnnMode::Off,
            false,
        )
        .unwrap();
        assert!(prepare(&d, &[1.0]).is_err());
        assert!(prepare(&d, &[0.0, 0.0]).is_err());
        let n = prepare(&d, &[3.0, 4.0]).unwrap();
        assert!((n[0] - 0.6).abs() < 1e-6);
        let (doc, _) = stored_metadata(&serde_json::json!({"a": 1, "t": ["x"]})).unwrap();
        assert!(doc.contains("\"a\":1"));
        assert!(stored_metadata(&serde_json::json!([1])).is_err());
        let back = metadata_doc(Some(doc.as_bytes()), None);
        assert_eq!(back.get_i32("a").ok(), Some(1));
    }
}
