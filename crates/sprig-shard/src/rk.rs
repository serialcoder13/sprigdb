//! The invisible `_sprig_rk` column (chapter 15, "The `_sprig_rk`
//! column", "Filling and back-filling `_sprig_rk`").
//!
//! It holds a row's routing hash, computed from its own key or copied
//! from its parent. These are the statements that add it, fill it and
//! back-fill it; the gateway fills it on writes through the router
//! (chapter 17) and the `backfill_rk` mover job runs the back-fill.

use sprig_common::names::quote;

use crate::key::RoutingHash;

/// The column's name.
pub const RK_COLUMN: &str = "_sprig_rk";

/// `ALTER TABLE t ADD COLUMN _sprig_rk ... INVISIBLE, ADD KEY _sprig_rk`.
/// Idempotent (`IF NOT EXISTS`).
#[must_use]
pub fn add_rk_column(table: &str) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {RK_COLUMN} BIGINT UNSIGNED NULL INVISIBLE, \
         ADD KEY IF NOT EXISTS {RK_COLUMN} ({RK_COLUMN})",
        quote(table)
    )
}

/// The value to write for a row: the hash as `BIGINT UNSIGNED`.
#[must_use]
pub fn rk_value(h: RoutingHash) -> u64 {
    h.0
}

/// One chunk's back-fill for a column member: `n` rows, parameters
/// `(pk, hash)` pairs, then the `n` primary keys again.
///
/// ```sql
/// UPDATE t SET _sprig_rk = CASE pk WHEN ? THEN ? ... END
///  WHERE pk IN (?, ...) AND _sprig_rk IS NULL
/// ```
///
/// The `IS NULL` guard keeps a value a gateway wrote after the chunk was
/// read. Single-column primary keys only; composite keys use row
/// constructors (`(a, b) = (?, ?)`), see [`backfill_columns_sql_composite`].
#[must_use]
pub fn backfill_columns_sql(table: &str, pk: &str, n: usize) -> String {
    let pk = quote(pk);
    let whens = " WHEN ? THEN ?".repeat(n);
    let ins = vec!["?"; n].join(", ");
    format!(
        "UPDATE {} SET {RK_COLUMN} = CASE {pk}{whens} END \
         WHERE {pk} IN ({ins}) AND {RK_COLUMN} IS NULL",
        quote(table)
    )
}

/// [`backfill_columns_sql`] for a composite primary key: parameters are,
/// per row, the key parts then the hash; then every row's key parts again.
#[must_use]
pub fn backfill_columns_sql_composite(table: &str, pk: &[String], n: usize) -> String {
    let cols = pk.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", ");
    let tuple = format!("({})", vec!["?"; pk.len()].join(", "));
    let whens = format!(" WHEN ({cols}) = {tuple} THEN ?").repeat(n);
    let ins = vec![tuple.as_str(); n].join(", ");
    format!(
        "UPDATE {} SET {RK_COLUMN} = CASE{whens} END \
         WHERE ({cols}) IN ({ins}) AND {RK_COLUMN} IS NULL",
        quote(table)
    )
}

/// The back-fill of a parent member: the parent is co-located, so its
/// hash is copied with a join. Parameters: the low and high child key of
/// the chunk (`child_range_col BETWEEN ? AND ?`).
///
/// `on` pairs child columns with parent columns.
#[must_use]
pub fn backfill_parent_sql(
    child: &str,
    parent: &str,
    on: &[(String, String)],
    child_range_col: &str,
) -> String {
    let cond = on
        .iter()
        .map(|(c, p)| format!("p.{} = i.{}", quote(p), quote(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "UPDATE {} i JOIN {} p ON {cond} SET i.{RK_COLUMN} = p.{RK_COLUMN} \
         WHERE i.{RK_COLUMN} IS NULL AND i.{} BETWEEN ? AND ?",
        quote(child),
        quote(parent),
        quote(child_range_col)
    )
}

/// Rows of a shard whose hash is outside its range `[lo, hi)`: a
/// misrouted row. `hi = None` is 2^64. Used by tests and the mover's
/// verification.
#[must_use]
pub fn misrouted_count_sql(table: &str, lo: u64, hi: Option<u64>) -> String {
    match hi {
        Some(hi) => format!(
            "SELECT COUNT(*) FROM {} WHERE {RK_COLUMN} IS NULL OR {RK_COLUMN} < {lo} \
             OR {RK_COLUMN} >= {hi}",
            quote(table)
        ),
        None => format!(
            "SELECT COUNT(*) FROM {} WHERE {RK_COLUMN} IS NULL OR {RK_COLUMN} < {lo}",
            quote(table)
        ),
    }
}

/// The routing hash of key values read from MariaDB (text or binary
/// protocol, or a binlog row image) under `spec`: what `_sprig_rk` holds
/// for a row whose key columns hold `vals`. The advisor's back-fill and
/// the mover's re-key (chapter 16) hash rows with it.
///
/// # Errors
/// 39400 for a value the class can't take; 39405 for one that can't be
/// normalized.
pub fn hash_values(
    spec: &crate::KeySpec,
    vals: &[mysql_async::Value],
) -> sprig_common::Result<u64> {
    use mysql_async::Value;
    use sprig_common::{SprigCode, SprigError};

    use crate::key::{DateTimeParts, DecimalRef, KeyClass, KeyValue};
    let texts: Vec<String> = vals
        .iter()
        .map(|v| match v {
            Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            _ => String::new(),
        })
        .collect();
    let mut kv: Vec<KeyValue<'_>> = Vec::with_capacity(vals.len());
    for (i, (class, v)) in spec.parts.iter().zip(vals).enumerate() {
        let bad = || {
            SprigError::new(
                SprigCode::UnsupportedKeyType,
                format!("key part {i}: {v:?} isn't a {class:?}"),
            )
        };
        kv.push(match (class, v) {
            (_, Value::NULL) => KeyValue::Null,
            (KeyClass::Numeric, Value::Int(x)) => KeyValue::Int(i128::from(*x)),
            (KeyClass::Numeric, Value::UInt(x)) => KeyValue::Int(i128::from(*x)),
            (KeyClass::Numeric, Value::Double(x)) => KeyValue::Double(*x),
            (KeyClass::Numeric, Value::Float(x)) => KeyValue::Double(f64::from(*x)),
            (KeyClass::Numeric, Value::Bytes(_)) => KeyValue::Decimal(DecimalRef(&texts[i])),
            (KeyClass::String { collation: c }, Value::Bytes(b)) => KeyValue::Str {
                bytes: b,
                charset: crate::collation::collation(*c)
                    .map_or(crate::Charset::Utf8mb4, |x| x.charset),
            },
            (KeyClass::Binary, Value::Bytes(b)) => KeyValue::Bytes(b),
            (KeyClass::Uuid, Value::Bytes(b)) => {
                let u: [u8; 16] = match <[u8; 16]>::try_from(b.as_slice()) {
                    Ok(u) => u,
                    Err(_) => crate::key::parse_uuid(&texts[i]).ok_or_else(bad)?,
                };
                KeyValue::Uuid(u)
            }
            (KeyClass::DateTime, Value::Date(y, mo, d, h, mi, s, us)) => {
                KeyValue::DateTime(DateTimeParts {
                    year: *y,
                    month: *mo,
                    day: *d,
                    hour: *h,
                    minute: *mi,
                    second: *s,
                    micros: *us,
                })
            }
            (KeyClass::Timestamp, Value::Int(secs)) => KeyValue::Timestamp {
                secs: *secs,
                micros: 0,
            },
            _ => return Err(bad()),
        });
    }
    if kv.len() != spec.parts.len() {
        return Err(SprigError::new(
            SprigCode::UnsupportedKeyType,
            format!(
                "{} key values for a key of {} parts",
                kv.len(),
                spec.parts.len()
            ),
        ));
    }
    Ok(crate::hash_key(spec, &kv)?.0.0)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn row_values_hash_like_literals() {
        let spec = crate::KeySpec::new(vec![crate::KeyClass::Numeric]).unwrap();
        let lit = crate::hash_key(&spec, &[crate::KeyValue::Int(42)])
            .map(|(h, _)| h.0)
            .ok();
        assert_eq!(hash_values(&spec, &[mysql_async::Value::Int(42)]).ok(), lit);
        assert_eq!(
            hash_values(&spec, &[mysql_async::Value::UInt(42)]).ok(),
            lit
        );
        assert!(hash_values(&spec, &[]).is_err());
    }

    #[test]
    fn statements() {
        assert_eq!(
            add_rk_column("orders"),
            "ALTER TABLE `orders` ADD COLUMN IF NOT EXISTS _sprig_rk BIGINT UNSIGNED NULL \
             INVISIBLE, ADD KEY IF NOT EXISTS _sprig_rk (_sprig_rk)"
        );
        assert_eq!(
            backfill_columns_sql("orders", "id", 2),
            "UPDATE `orders` SET _sprig_rk = CASE `id` WHEN ? THEN ? WHEN ? THEN ? END \
             WHERE `id` IN (?, ?) AND _sprig_rk IS NULL"
        );
        assert_eq!(
            backfill_columns_sql_composite("t", &["a".into(), "b".into()], 1),
            "UPDATE `t` SET _sprig_rk = CASE WHEN (`a`, `b`) = (?, ?) THEN ? END \
             WHERE (`a`, `b`) IN ((?, ?)) AND _sprig_rk IS NULL"
        );
        assert_eq!(
            backfill_parent_sql(
                "order_items",
                "orders",
                &[("order_id".into(), "id".into())],
                "order_id"
            ),
            "UPDATE `order_items` i JOIN `orders` p ON p.`id` = i.`order_id` \
             SET i._sprig_rk = p._sprig_rk WHERE i._sprig_rk IS NULL AND i.`order_id` \
             BETWEEN ? AND ?"
        );
    }
}
