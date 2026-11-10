//! Where models live, and running a query (chapter 35, "Catalog tables",
//! "Type 2 dimensions with system versioning").
//!
//! As built in R6 a model is one row of `<db>.__sprig_dim_models` in its
//! own logical database (the definition as submitted, a version, a state),
//! made on first use like the key-value engine's tables, instead of the
//! tenant catalog's `dim_models`, `dimensions`, `dim_levels`, `facts` and
//! `measures`. Every statement runs through the caller's session, so a
//! user needs `SELECT` on the tables a model reads.

use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_kv::SqlRunner;
use sprig_kv::sql::cell_u64;

use crate::merge::{self, OutRow, Spec};
use crate::model::{DimKind, Model, Versioning, invalid};
use crate::query::{Compiled, Query, compile};

fn backend(e: sprig_kv::SqlError) -> SprigError {
    SprigError::standard(e.errno.unwrap_or(1105), *b"HY000", e.message)
}

fn text(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

fn table(db: &str) -> String {
    format!("`{}`.`__sprig_dim_models`", db.replace('`', "``"))
}

/// A stored model.
#[derive(Debug, Clone, PartialEq)]
pub struct Stored {
    /// The parsed model.
    pub model: Model,
    /// The definition as submitted.
    pub definition: Value,
    /// Bumped by every update.
    pub version: u64,
    /// `active` or `invalid`.
    pub state: String,
}

async fn ensure(sql: &mut dyn SqlRunner, db: &str) -> Result<()> {
    sql.exec(&format!(
        "CREATE DATABASE IF NOT EXISTS `{}`",
        db.replace('`', "``")
    ))
    .await
    .map_err(backend)?;
    sql.exec(&format!(
        "CREATE TABLE IF NOT EXISTS {} (name VARCHAR(64) NOT NULL PRIMARY KEY, definition LONGTEXT NOT NULL, \
         version BIGINT UNSIGNED NOT NULL, state VARCHAR(16) NOT NULL, created_at DATETIME(3) NOT NULL, \
         updated_at DATETIME(3) NOT NULL)",
        table(db)
    ))
    .await
    .map_err(backend)?;
    Ok(())
}

/// Checks a model against the database: every table and column exists,
/// and type 2 dimensions are versioned (system versioning is added to a
/// table that lacks it, as chapter 35 says). The problems, empty when
/// none.
///
/// # Errors
/// The backend's.
pub async fn validate(
    sql: &mut dyn SqlRunner,
    m: &Model,
    add_versioning: bool,
) -> Result<Vec<String>> {
    let rows = sql
        .query(&format!(
            "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = {}",
            text(&m.database)
        ))
        .await
        .map_err(backend)?;
    let have: std::collections::HashSet<(String, String)> = rows
        .iter()
        .filter_map(|r| {
            Some((
                String::from_utf8_lossy(r.first()?.as_ref()?).to_ascii_lowercase(),
                String::from_utf8_lossy(r.get(1)?.as_ref()?).to_ascii_lowercase(),
            ))
        })
        .collect();
    let mut problems = Vec::new();
    for (t, c) in m.columns() {
        if !have.contains(&(t.to_ascii_lowercase(), c.to_ascii_lowercase())) {
            problems.push(format!("no column {t}.{c}"));
        }
    }
    for d in &m.dimensions {
        if let DimKind::Table {
            table: t,
            versioning: Versioning::System,
            ..
        } = &d.kind
        {
            let r = sql
                .query(&format!(
                    "SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {}",
                    text(&m.database),
                    text(t)
                ))
                .await
                .map_err(backend)?;
            let kind = r
                .first()
                .and_then(|r| r.first())
                .and_then(Clone::clone)
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .unwrap_or_default();
            if kind == "SYSTEM VERSIONED" {
                continue;
            }
            if kind.is_empty() {
                continue; // reported as missing columns
            }
            if add_versioning {
                if let Err(e) = sql
                    .exec(&format!(
                        "ALTER TABLE `{}`.`{}` ADD SYSTEM VERSIONING",
                        m.database.replace('`', "``"),
                        t.replace('`', "``")
                    ))
                    .await
                {
                    problems.push(format!(
                        "dimension '{}': system versioning couldn't be added: {e}",
                        d.name
                    ));
                }
            } else {
                problems.push(format!(
                    "dimension '{}': table {t} isn't system-versioned",
                    d.name
                ));
            }
        }
    }
    Ok(problems)
}

/// Creates (or, with `replace`, updates) a model.
///
/// # Errors
/// 39752 for an invalid model; 1062-like when it exists; the backend's.
pub async fn put(
    sql: &mut dyn SqlRunner,
    db: &str,
    definition: &Value,
    replace: bool,
) -> Result<Stored> {
    let model = Model::parse(db, definition)?;
    ensure(sql, db).await?;
    let problems = validate(sql, &model, true).await?;
    if !problems.is_empty() {
        return Err(invalid(format!(
            "model '{}': {}",
            model.name,
            problems.join("; ")
        )));
    }
    let def = definition.to_string();
    if replace {
        let n = sql
            .exec(&format!(
                "UPDATE {} SET definition = {}, version = version + 1, state = 'active', updated_at = UTC_TIMESTAMP(3) WHERE name = {}",
                table(db),
                text(&def),
                text(&model.name)
            ))
            .await
            .map_err(backend)?;
        if n == 0 {
            return Err(SprigError::new(
                SprigCode::DimModelNotFound,
                format!("no model '{}'", model.name),
            ));
        }
    } else {
        sql.exec(&format!(
            "INSERT INTO {} VALUES ({}, {}, 1, 'active', UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
            table(db),
            text(&model.name),
            text(&def)
        ))
        .await
        .map_err(|e| {
            if e.errno == Some(1062) {
                SprigError::new(
                    SprigCode::DimInvalidModel,
                    format!("model '{}' exists", model.name),
                )
            } else {
                backend(e)
            }
        })?;
    }
    get(sql, db, &model.name).await
}

/// Reads a model.
///
/// # Errors
/// 39750 when there is none; the backend's.
pub async fn get(sql: &mut dyn SqlRunner, db: &str, name: &str) -> Result<Stored> {
    let rows = match sql
        .query(&format!(
            "SELECT definition, version, state FROM {} WHERE name = {}",
            table(db),
            text(name)
        ))
        .await
    {
        Ok(r) => r,
        Err(e) if matches!(e.errno, Some(1146 | 1049)) => Vec::new(),
        Err(e) => return Err(backend(e)),
    };
    let r = rows.first().ok_or_else(|| {
        SprigError::new(
            SprigCode::DimModelNotFound,
            format!("no model '{name}' in '{db}'"),
        )
    })?;
    let definition: Value = r
        .first()
        .and_then(Clone::clone)
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or(Value::Null);
    Ok(Stored {
        model: Model::parse(db, &definition)?,
        definition,
        version: cell_u64(r.get(1).and_then(Option::as_ref)),
        state: r
            .get(2)
            .and_then(Clone::clone)
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default(),
    })
}

/// The models of a database, by name.
///
/// # Errors
/// The backend's.
pub async fn list(sql: &mut dyn SqlRunner, db: &str) -> Result<Vec<Stored>> {
    let rows = match sql
        .query(&format!("SELECT name FROM {} ORDER BY name", table(db)))
        .await
    {
        Ok(r) => r,
        Err(e) if matches!(e.errno, Some(1146 | 1049)) => return Ok(Vec::new()),
        Err(e) => return Err(backend(e)),
    };
    let mut out = Vec::new();
    for r in rows {
        if let Some(Some(n)) = r.first() {
            out.push(get(sql, db, &String::from_utf8_lossy(n)).await?);
        }
    }
    Ok(out)
}

/// Drops a model (its tables stay).
///
/// # Errors
/// 39750 when there is none; the backend's.
pub async fn drop(sql: &mut dyn SqlRunner, db: &str, name: &str) -> Result<()> {
    let n = match sql
        .exec(&format!(
            "DELETE FROM {} WHERE name = {}",
            table(db),
            text(name)
        ))
        .await
    {
        Ok(n) => n,
        Err(e) if matches!(e.errno, Some(1146 | 1049)) => 0,
        Err(e) => return Err(backend(e)),
    };
    if n == 0 {
        return Err(SprigError::new(
            SprigCode::DimModelNotFound,
            format!("no model '{name}'"),
        ));
    }
    Ok(())
}

/// The answer to a query, with how it ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// Rows: detail rows, subtotals after their groups, the grand total last.
    pub rows: Vec<OutRow>,
    /// The compiled statements.
    pub compiled: Compiled,
}

/// Runs a query.
///
/// # Errors
/// 39750-39754 and the backend's; a model whose tables changed is marked
/// `invalid` and refused with 39752.
pub async fn query(
    sql: &mut dyn SqlRunner,
    db: &str,
    q: &Query,
    max_groups: usize,
) -> Result<Answer> {
    let stored = get(sql, db, &q.model).await?;
    if stored.state == "invalid" {
        let problems = validate(sql, &stored.model, false).await?;
        if !problems.is_empty() {
            return Err(invalid(format!(
                "model '{}' is invalid: {}",
                q.model,
                problems.join("; ")
            )));
        }
        sql.exec(&format!(
            "UPDATE {} SET state = 'active' WHERE name = {}",
            table(db),
            text(&q.model)
        ))
        .await
        .map_err(backend)?;
    }
    let c = compile(&stored.model, q)?;
    let detail = match sql.query(&c.sql).await {
        Ok(r) => r,
        Err(e) if matches!(e.errno, Some(1054 | 1146)) => {
            let _ = sql
                .exec(&format!(
                    "UPDATE {} SET state = 'invalid' WHERE name = {}",
                    table(db),
                    text(&q.model)
                ))
                .await;
            return Err(invalid(format!("model '{}' is invalid: {e}", q.model)));
        }
        Err(e) => return Err(backend(e)),
    };
    let mut level_rows = Vec::with_capacity(c.levels.len());
    for s in &c.levels {
        level_rows.push(sql.query(s).await.map_err(backend)?);
    }
    // Chapter 04 (decision bugfix-1): a large merge runs off the
    // runtime's worker threads.
    let n = detail.len() + level_rows.iter().map(Vec::len).sum::<usize>();
    let rows = sprig_common::cpu::heavy(n, || {
        let levels = level_rows
            .iter()
            .enumerate()
            .map(|(j, r)| merge::rows_of(&c, j, r))
            .collect();
        merge::merge(
            &Spec {
                compiled: &c,
                measures: &q.measures,
                order: &q.order,
                totals: q.totals,
                limit: q.limit,
                max_groups,
            },
            merge::rows_of(&c, c.keys, &detail),
            levels,
        )
    })?;
    Ok(Answer { rows, compiled: c })
}

/// Chapter 35's drill: the next query down (`up = false`: replace the
/// dimension's level `l` by `l + 1` and filter `l = member`) or up
/// (replace `l` by `l − 1` and drop filters on finer levels).
///
/// # Errors
/// 39751 when the dimension isn't grouped or there's no level to go to.
pub fn drill(
    m: &Model,
    q: &Query,
    dimension: &str,
    member: Option<&Value>,
    up: bool,
) -> Result<Query> {
    let d = m
        .dimension(dimension)
        .ok_or_else(|| crate::query::unknown(format!("unknown dimension '{dimension}'")))?;
    let levels = d.levels();
    let i = q
        .dims
        .iter()
        .position(|x| x.0 == dimension)
        .ok_or_else(|| {
            crate::query::unknown(format!("the query doesn't group by '{dimension}'"))
        })?;
    let l = levels
        .iter()
        .position(|x| *x == q.dims[i].1)
        .ok_or_else(|| crate::query::unknown("unknown level"))?;
    let mut out = q.clone();
    if up {
        if l == 0 {
            return Err(crate::query::unknown(format!(
                "'{dimension}' is at its top level"
            )));
        }
        out.dims[i].1.clone_from(&levels[l - 1]);
        out.filters.retain(|f| {
            f.dimension != dimension
                || levels
                    .iter()
                    .position(|x| *x == f.level)
                    .is_some_and(|p| p < l - 1)
        });
    } else {
        if l + 1 >= levels.len() {
            return Err(crate::query::unknown(format!(
                "'{dimension}' is at its finest level"
            )));
        }
        out.dims[i].1.clone_from(&levels[l + 1]);
        if let Some(v) = member {
            out.filters.push(crate::query::Filter {
                dimension: dimension.to_owned(),
                level: levels[l].clone(),
                op: crate::query::Op::Eq,
                values: vec![v.clone()],
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use serde_json::json;
    use sprig_kv::SqlError;

    use super::*;

    /// Answers the model row and a detail of `n` groups.
    struct Fake {
        n: usize,
    }

    #[async_trait::async_trait]
    impl SqlRunner for Fake {
        async fn query(&mut self, sql: &str) -> std::result::Result<Vec<sprig_kv::Row>, SqlError> {
            if sql.starts_with("SELECT definition") {
                let d = json!({"model": "sales", "time_zone": "UTC",
                    "dimensions": [{"name": "product", "table": "product", "key": "id", "levels": ["cat"]}],
                    "facts": [{"name": "sales", "table": "sales", "key": ["id"],
                        "links": [{"dimension": "product", "column": "product_id"}],
                        "measures": [{"name": "revenue", "expr": "amount", "agg": "sum"}]}]});
                return Ok(vec![vec![
                    Some(d.to_string().into_bytes()),
                    Some(b"1".to_vec()),
                    Some(b"active".to_vec()),
                ]]);
            }
            Ok((0..self.n)
                .map(|i| vec![Some(format!("c{i:07}").into_bytes()), Some(b"12.50".to_vec())])
                .collect())
        }

        async fn exec(&mut self, _sql: &str) -> std::result::Result<u64, SqlError> {
            Ok(0)
        }
    }

    /// Two large subtotal merges at once on a two-worker runtime leave a
    /// ticker on it running (chapter 04, decision bugfix-1). Before they
    /// were offloaded, each held a worker for the whole merge.
    #[test]
    fn large_merges_leave_the_workers_free() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let stop = Arc::new(AtomicBool::new(false));
            let s2 = Arc::clone(&stop);
            let ticker = tokio::spawn(async move {
                let mut worst = Duration::ZERO;
                let mut last = Instant::now();
                while !s2.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    worst = worst.max(last.elapsed());
                    last = Instant::now();
                }
                worst
            });
            let q = Query::parse(&json!({"model": "sales", "measures": ["revenue"], "totals": true,
                "dimensions": [{"dimension": "product", "level": "cat"}]}))
            .unwrap();
            let run = |q: Query| {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    let t = Instant::now();
                    let a = query(&mut Fake { n: 50_000 }, "shop", &q, 1_000_000)
                        .await
                        .unwrap();
                    (a.rows.len(), t.elapsed())
                })
            };
            let (a, b) = (run(q.clone()), run(q));
            let (ra, ta) = a.await.unwrap();
            let (rb, _) = b.await.unwrap();
            stop.store(true, Ordering::Relaxed);
            let worst = ticker.await.unwrap();
            assert_eq!(ra, 50_001, "every group and the total");
            assert_eq!(rb, 50_001);
            assert!(
                worst < Duration::from_millis(200),
                "ticker stalled {worst:?} while each merge took {ta:?}"
            );
        });
    }
}
