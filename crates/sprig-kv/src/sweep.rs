//! The expiry and old-generation sweeper (chapter 33, "Expiry and
//! garbage"; chapter 40, "Expiry: lazy and active").
//!
//! Reads hide expired keys at once; this removes their rows. It runs in a
//! worker as `sprig_ctl` over a cluster's physical schemas:
//!
//! ```text
//! namespaces: every __sprig_ke_<name> table with its key table <name>, in
//!   tenant schemas (t<id>_...)
//! expired(ns), while a full batch was found:
//!   START TRANSACTION (READ COMMITTED: no gap locks)
//!   SELECT k, g, n FROM ns WHERE expires_at <= UTC_TIMESTAMP(3)
//!     ORDER BY expires_at LIMIT batch FOR UPDATE SKIP LOCKED   -- clients' keys pass
//!   DELETE FROM ns WHERE k IN (...) AND expires_at <= UTC_TIMESTAMP(3) /* sprig r=expired */
//!   delete the elements of keys with n <= inline_max by (k, g); COMMIT
//!   larger keys: their elements by (k, g), batch rows per statement, after commit
//! orphans(ns), every orphan interval: element rows whose (k, g) has no key
//!   row with that generation, deleted by (k, g)
//! ```
//!
//! A generation is never reused (a new key gets a fresh `_v`), so deleting
//! by `(k, g)` never touches a live key's elements, and element rows of an
//! uncommitted write aren't visible to the sweeper's reads.

use crate::sql::{SqlError, SqlRunner, cell_i64, cell_u64, ident, lit};

/// The sweeper's limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepOpts {
    /// `kv.purge_batch_rows`: keys per transaction, element rows per delete.
    pub batch_rows: u64,
    /// `kv.inline_delete_max_elements`: larger keys' elements are deleted
    /// after the key row's transaction.
    pub inline_max: u64,
}

impl Default for SweepOpts {
    fn default() -> Self {
        Self {
            batch_rows: 500,
            inline_max: 128,
        }
    }
}

/// A namespace's physical tables.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NsTables {
    /// The schema (`t<tenant>_<database>`).
    pub schema: String,
    /// The key table; the element tables derive from it.
    pub table: String,
}

impl NsTables {
    fn keys(&self) -> String {
        format!("{}.{}", ident(&self.schema), ident(&self.table))
    }
    fn elems(&self) -> String {
        format!(
            "{}.{}",
            ident(&self.schema),
            ident(&format!("__sprig_ke_{}", self.table))
        )
    }
    fn zset(&self) -> String {
        format!(
            "{}.{}",
            ident(&self.schema),
            ident(&format!("__sprig_kz_{}", self.table))
        )
    }
}

/// What one pass removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    /// Expired key rows.
    pub keys: u64,
    /// Element rows (hash fields, list items, members).
    pub elements: u64,
}

impl std::ops::AddAssign for Swept {
    fn add_assign(&mut self, o: Self) {
        self.keys += o.keys;
        self.elements += o.elements;
    }
}

/// Every namespace on the server: tables `__sprig_ke_<name>` whose key
/// table `<name>` exists, in tenant schemas.
///
/// # Errors
/// The server's errors.
pub async fn discover(sql: &mut dyn SqlRunner) -> Result<Vec<NsTables>, SqlError> {
    let rows = sql
        .query(
            "SELECT e.table_schema, SUBSTRING(e.table_name, 12) \
             FROM information_schema.tables e \
             JOIN information_schema.tables k \
               ON k.table_schema = e.table_schema \
              AND k.table_name = SUBSTRING(e.table_name, 12) \
             WHERE e.table_name LIKE '\\_\\_sprig\\_ke\\_%' \
               AND e.table_schema REGEXP '^t[0-9]+_' \
             ORDER BY 1, 2",
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let s = String::from_utf8(r.first()?.clone()?).ok()?;
            let t = String::from_utf8(r.get(1)?.clone()?).ok()?;
            Some(NsTables {
                schema: s,
                table: t,
            })
        })
        .collect())
}

/// Milliseconds until the namespace's next expiry (0 if one is due),
/// `None` without TTLs.
///
/// # Errors
/// The server's errors.
pub async fn next_expiry_ms(
    sql: &mut dyn SqlRunner,
    ns: &NsTables,
) -> Result<Option<i64>, SqlError> {
    let rows = sql
        .query(&format!(
            "SELECT GREATEST(0, TIMESTAMPDIFF(MICROSECOND, UTC_TIMESTAMP(3), MIN(expires_at)) DIV 1000) \
             FROM {} WHERE expires_at IS NOT NULL",
            ns.keys()
        ))
        .await?;
    Ok(rows
        .first()
        .and_then(|r| r.first())
        .and_then(Clone::clone)
        .map(|c| cell_i64(Some(&c))))
}

/// One batch of expired keys: deleted with their elements. The count of
/// keys found tells whether to run again (a full batch).
///
/// # Errors
/// The server's errors (the transaction is rolled back).
pub async fn expired_batch(
    sql: &mut dyn SqlRunner,
    ns: &NsTables,
    o: SweepOpts,
) -> Result<(u64, Swept), SqlError> {
    sql.exec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await?;
    sql.exec("START TRANSACTION").await?;
    let r = async {
        let rows = sql
            .query(&format!(
                "SELECT k, g, n FROM {} WHERE expires_at <= UTC_TIMESTAMP(3) \
                 ORDER BY expires_at LIMIT {} FOR UPDATE SKIP LOCKED",
                ns.keys(),
                o.batch_rows.max(1)
            ))
            .await?;
        let found = rows.len() as u64;
        if rows.is_empty() {
            return Ok((0, Swept::default(), Vec::new()));
        }
        let keys: Vec<(Vec<u8>, u64, u64)> = rows
            .into_iter()
            .filter_map(|r| {
                Some((
                    r.first()?.clone()?,
                    cell_u64(r.get(1)?.as_ref()),
                    cell_u64(r.get(2)?.as_ref()),
                ))
            })
            .collect();
        let list: Vec<String> = keys.iter().map(|(k, _, _)| lit(k)).collect();
        let deleted = sql
            .exec(&format!(
                "DELETE FROM {} WHERE k IN ({}) AND expires_at <= UTC_TIMESTAMP(3) /* sprig r=expired */",
                ns.keys(),
                list.join(", ")
            ))
            .await?;
        let small: Vec<String> = keys
            .iter()
            .filter(|(_, _, n)| *n > 0 && *n <= o.inline_max)
            .map(|(k, g, _)| format!("({}, {g})", lit(k)))
            .collect();
        let mut elements = 0;
        if !small.is_empty() {
            for t in [ns.elems(), ns.zset()] {
                elements += sql
                    .exec(&format!(
                        "DELETE FROM {t} WHERE (k, g) IN ({})",
                        small.join(", ")
                    ))
                    .await?;
            }
        }
        let large: Vec<(Vec<u8>, u64)> = keys
            .into_iter()
            .filter(|(_, _, n)| *n > o.inline_max)
            .map(|(k, g, _)| (k, g))
            .collect();
        Ok((
            found,
            Swept {
                keys: deleted,
                elements,
            },
            large,
        ))
    }
    .await;
    match r {
        Ok((found, mut swept, large)) => {
            sql.exec("COMMIT").await?;
            for (k, g) in large {
                swept.elements += drop_generation(sql, ns, &k, g, o).await?;
            }
            Ok((found, swept))
        }
        Err(e) => {
            let _ = sql.exec("ROLLBACK").await;
            Err(e)
        }
    }
}

/// Deletes one garbage generation's elements, a batch per statement.
///
/// # Errors
/// The server's errors.
pub async fn drop_generation(
    sql: &mut dyn SqlRunner,
    ns: &NsTables,
    key: &[u8],
    generation: u64,
    opts: SweepOpts,
) -> Result<u64, SqlError> {
    let batch = opts.batch_rows.max(1);
    let mut total = 0;
    for table in [ns.elems(), ns.zset()] {
        loop {
            let deleted = sql
                .exec(&format!(
                    "DELETE FROM {table} WHERE k = {} AND g = {generation} LIMIT {batch}",
                    lit(key)
                ))
                .await?;
            total += deleted;
            if deleted < batch {
                break;
            }
        }
    }
    Ok(total)
}

/// Every expired key of a namespace, batch by batch.
///
/// # Errors
/// The server's errors.
pub async fn expired(
    sql: &mut dyn SqlRunner,
    ns: &NsTables,
    o: SweepOpts,
) -> Result<Swept, SqlError> {
    let mut total = Swept::default();
    loop {
        let (found, s) = expired_batch(sql, ns, o).await?;
        total += s;
        if found < o.batch_rows.max(1) {
            return Ok(total);
        }
    }
}

/// Element rows of generations no key row has: up to `batch_rows`
/// generations per call, each deleted by `(k, g)`. Returns the element
/// rows deleted and the generations found.
///
/// # Errors
/// The server's errors.
pub async fn orphans(
    sql: &mut dyn SqlRunner,
    ns: &NsTables,
    o: SweepOpts,
) -> Result<(u64, u64), SqlError> {
    let mut gens = Vec::new();
    for t in [ns.elems(), ns.zset()] {
        let rows = sql
            .query(&format!(
                "SELECT DISTINCT e.k, e.g FROM {t} e LEFT JOIN {} kt \
                 ON kt.k = e.k AND kt.g = e.g WHERE kt.k IS NULL LIMIT {}",
                ns.keys(),
                o.batch_rows.max(1)
            ))
            .await?;
        for r in rows {
            if let (Some(Some(k)), Some(g)) = (r.first(), r.get(1)) {
                gens.push((k.clone(), cell_u64(g.as_ref())));
            }
        }
    }
    gens.sort();
    gens.dedup();
    let mut n = 0;
    for (k, g) in &gens {
        n += drop_generation(sql, ns, k, *g, o).await?;
    }
    Ok((n, gens.len() as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_are_quoted() {
        let ns = NsTables {
            schema: "t7_redis".into(),
            table: "db0".into(),
        };
        assert_eq!(ns.keys(), "`t7_redis`.`db0`");
        assert_eq!(ns.elems(), "`t7_redis`.`__sprig_ke_db0`");
        assert_eq!(ns.zset(), "`t7_redis`.`__sprig_kz_db0`");
    }
}
