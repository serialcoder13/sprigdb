//! Input assembly (chapter 16, "Inputs and the window"; build step 1):
//! tables, columns, keys, sizes and declared foreign keys from the
//! tenant's schemas on its cluster; statements and transaction footprints
//! from chapter 12's workload store in its catalog.
//!
//! Before a first split every table of a logical database lives in the
//! database's default-group shard schema, `t<tenant>_d<db>_g<g>_s<s>`, on
//! the tenant's cluster; the catalog's `objects` rows give each table its
//! object id. The tenant's pins come from chapter 15's `group_pins`.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_common::ids::{DbId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_shard::KeyClass;
use sprig_shard::pins::{Pin, PinKind, PinStore};
use sprig_workload::fingerprint::FpKind;
use sprig_workload::shape::{JoinOp, PredOp, Shape, WriteKind};
use sprig_workload::store::WorkloadTables;

use sprig_shard::colocate::SqlType;

use super::design::{Column, ForeignKey, Input, JoinEdge, Obj, Rules, Stmt, Table, Touch, Unit};

fn db_err(what: &str, e: &mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, format!("{what}: {e}"))
}

/// A table's place before the first design.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Located {
    /// The object.
    pub object: Obj,
    /// The database.
    pub db: DbId,
    /// Its schema now (the database's default-group shard schema).
    pub schema: String,
    /// The table.
    pub name: String,
}

/// What the input was read from, with where each table is.
#[derive(Debug, Clone, Default)]
pub struct Assembled {
    /// The advisor's input.
    pub input: Input,
    /// Each table's schema.
    pub located: Vec<Located>,
    /// The oldest statement in the window.
    pub first_seen: Option<SystemTime>,
    /// The catalog version the run read.
    pub schema_version: u64,
}

/// The key class of a column type, or `None` (the rule `SPRIG PIN`
/// checks with, `sprig_shard::pins::column_class`).
#[must_use]
pub fn class_of(data_type: &str, collation: Option<&str>) -> Option<KeyClass> {
    sprig_shard::pins::column_class(data_type, collation)
}

fn quote_list(v: &[String]) -> String {
    v.iter()
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Each database's default-group schema on the connection's server, from
/// the catalog's `databases.default_group_id` and the server's schemas.
async fn default_schemas(
    c: &mut Conn,
    tenant: TenantId,
    catalog: &str,
) -> Result<Vec<(DbId, String)>> {
    let dbs: Vec<(u32, u32)> = c
        .query(format!(
            "SELECT db_id, default_group_id FROM `{catalog}`.`databases` WHERE state = 'active'"
        ))
        .await
        .map_err(|e| db_err("databases", &e))?;
    let names: Vec<String> = c
        .exec(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE ?",
            (format!("t{tenant}\\_d%"),),
        )
        .await
        .map_err(|e| db_err("schemata", &e))?;
    // Groups a first design is still applying (a resumed run): their
    // members may already sit in the group's first shard.
    let preparing: Vec<u32> = c
        .query(format!(
            "SELECT group_id FROM `{catalog}`.table_groups WHERE state = 'preparing'"
        ))
        .await
        .unwrap_or_default();
    let mut out = Vec::new();
    for (db, g) in dbs {
        let mut found: Vec<String> = names
            .iter()
            .filter(|n| {
                matches!(n.parse::<SchemaName>(), Ok(SchemaName::Shard { tenant: t, db: d, group, .. })
                    if t == tenant && d == DbId(db) && group.0 == g)
            })
            .cloned()
            .collect();
        found.sort();
        if let Some(s) = found.into_iter().next() {
            out.push((DbId(db), s));
        }
        for pg in &preparing {
            let s = SchemaName::shard(
                tenant,
                DbId(db),
                sprig_common::ids::GroupId(*pg),
                sprig_common::ids::ShardId(1),
            )
            .to_string();
            if names.contains(&s) {
                out.push((DbId(db), s));
            }
        }
    }
    Ok(out)
}

/// Reads the tables of the default-group schemas and their keys.
///
/// # Errors
/// MariaDB's.
#[allow(clippy::too_many_lines)]
pub async fn tables(
    c: &mut Conn,
    tenant: TenantId,
) -> Result<(Vec<Table>, Vec<ForeignKey>, Vec<Located>)> {
    let catalog = SchemaName::catalog(tenant).to_string();
    let schemas = default_schemas(c, tenant, &catalog).await?;
    let objects: Vec<(u64, u32, String)> = c
        .query(format!(
            "SELECT object_id, db_id, name FROM `{catalog}`.objects \
             WHERE namespace = 'relation' AND kind = 'table' AND state = 'active'"
        ))
        .await
        .map_err(|e| db_err("objects", &e))?;
    let obj = |db: DbId, name: &str| {
        objects
            .iter()
            .find(|(_, d, n)| *d == db.0 && n.eq_ignore_ascii_case(name))
            .map(|(o, _, _)| *o)
    };
    let mut tables = Vec::new();
    let mut located = Vec::new();
    let mut fks = Vec::new();
    for (db, schema) in &schemas {
        let s = quote_list(std::slice::from_ref(schema));
        let tbls: Vec<(String, u64)> = c
            .query(format!(
                "SELECT TABLE_NAME, CAST(DATA_LENGTH + INDEX_LENGTH AS UNSIGNED) \
                 FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ({s}) \
                 AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
            ))
            .await
            .map_err(|e| db_err("tables", &e))?;
        let cols: Vec<(String, String, String, Option<String>)> = c
            .query(format!(
                "SELECT TABLE_NAME, LOWER(COLUMN_NAME), DATA_TYPE, COLLATION_NAME \
                 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA IN ({s}) \
                 ORDER BY TABLE_NAME, ORDINAL_POSITION"
            ))
            .await
            .map_err(|e| db_err("columns", &e))?;
        let stats: Vec<(String, String, u64, u32, String, Option<u64>)> = c
            .query(format!(
                "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, LOWER(COLUMN_NAME), \
                 CARDINALITY FROM information_schema.STATISTICS WHERE TABLE_SCHEMA IN ({s}) \
                 ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
            ))
            .await
            .map_err(|e| db_err("statistics", &e))?;
        let refs: Vec<(String, String, String, String, String, u32)> = c
            .query(format!(
                "SELECT CONSTRAINT_NAME, TABLE_NAME, LOWER(COLUMN_NAME), REFERENCED_TABLE_NAME, \
                 LOWER(REFERENCED_COLUMN_NAME), ORDINAL_POSITION \
                 FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA IN ({s}) \
                 AND REFERENCED_TABLE_SCHEMA = TABLE_SCHEMA \
                 ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION"
            ))
            .await
            .map_err(|e| db_err("foreign keys", &e))?;
        for (name, bytes) in tbls {
            // Chapter 33's element tables follow their key table
            // ([`kv_followers`]); other internal tables stay out.
            if sprig_common::names::is_internal_object(&name) && kv_key_table(&name).is_none() {
                continue;
            }
            let Some(object) = obj(*db, &name) else {
                continue; // not in the catalog: not the tenant's table
            };
            // Indexes: name -> (unique, columns, cardinality of the first column).
            let mut idx: BTreeMap<String, (bool, Vec<String>, Option<u64>)> = BTreeMap::new();
            for (t, i, nu, seq, col, card) in &stats {
                if t != &name {
                    continue;
                }
                let e = idx.entry(i.clone()).or_insert((*nu == 0, Vec::new(), None));
                e.1.push(col.clone());
                if *seq == 1 {
                    e.2 = *card;
                }
            }
            let ndv = |col: &str| -> Option<f64> {
                idx.values()
                    .filter(|(_, cs, _)| cs.first().map(String::as_str) == Some(col))
                    .filter_map(|(_, _, card)| *card)
                    .max()
                    .map(|n| {
                        #[allow(clippy::cast_precision_loss)]
                        let f = n as f64;
                        f
                    })
            };
            let columns: Vec<Column> = cols
                .iter()
                .filter(|(t, ..)| t == &name)
                .map(|(_, c, ty, coll)| Column {
                    name: c.clone(),
                    class: class_of(ty, coll.as_deref()),
                    ndv: ndv(c),
                    top_share: None,
                    sql: SqlType::parse(ty, coll.as_deref()),
                })
                .collect();
            let pk = idx.get("PRIMARY").map(|x| x.1.clone()).unwrap_or_default();
            let uniques = idx
                .iter()
                .filter(|(n, (u, ..))| *u && n.as_str() != "PRIMARY")
                .map(|(_, (_, cs, _))| cs.clone())
                .collect();
            #[allow(clippy::cast_precision_loss)]
            tables.push(Table {
                object,
                db: db.0,
                name: name.clone(),
                columns,
                pk,
                uniques,
                size_bytes: bytes as f64,
            });
            located.push(Located {
                object,
                db: *db,
                schema: schema.clone(),
                name: name.clone(),
            });
        }
        let mut by_name: BTreeMap<(String, String), ForeignKey> = BTreeMap::new();
        for (cn, t, col, rt, rc, _) in refs {
            let (Some(child), Some(parent)) = (obj(*db, &t), obj(*db, &rt)) else {
                continue;
            };
            let e = by_name
                .entry((t.clone(), cn.clone()))
                .or_insert(ForeignKey {
                    name: cn.clone(),
                    child,
                    child_cols: Vec::new(),
                    parent,
                    parent_cols: Vec::new(),
                });
            e.child_cols.push(col);
            e.parent_cols.push(rc);
        }
        fks.extend(by_name.into_values());
    }
    Ok((tables, fks, located))
}

/// The key table of one of chapter 33's element tables (`__sprig_ke_<t>`,
/// `__sprig_kz_<t>`): `<t>`.
#[must_use]
pub fn kv_key_table(name: &str) -> Option<&str> {
    ["__sprig_ke_", "__sprig_kz_"].iter().find_map(|p| {
        (name.len() > p.len() && name.as_bytes()[..p.len()].eq_ignore_ascii_case(p.as_bytes()))
            .then(|| &name[p.len()..])
    })
}

/// The distribution-key column of chapter 33's tables.
const KV_DK: &str = "_sprig_dk";

/// Chapter 33's element tables in the same group as their key table, on
/// the same key (R6 round 4). Clients can't pin `__sprig_` tables, so the
/// pins are implied:
///
/// - an element table whose key table isn't in the input leaves it;
/// - a key table pinned `UNSHARDED` or `REFERENCE` keeps its element tables
///   unsharded;
/// - otherwise each element table is `COLOCATE WITH` its key table on the
///   key table's pinned key, `_sprig_dk` when it has no pin (the key table
///   then gets `SHARD KEY (_sprig_dk)`: hash-tagged keys and their
///   elements stay together, as Redis Cluster keeps them in one slot).
pub fn kv_followers(tables: &mut Vec<Table>, located: &mut Vec<Located>, pins: &mut Vec<Pin>) {
    use sprig_common::ids::ObjectId;
    let mut drop: Vec<Obj> = Vec::new();
    let mut add: Vec<Pin> = Vec::new();
    for e in tables.iter() {
        let Some(key_name) = kv_key_table(&e.name) else {
            continue;
        };
        let Some(k) = tables
            .iter()
            .find(|t| t.db == e.db && t.name.eq_ignore_ascii_case(key_name))
        else {
            drop.push(e.object);
            continue;
        };
        if pins.iter().any(|p| p.object.0 == e.object) {
            continue;
        }
        let key_pin = pins.iter().find(|p| p.object.0 == k.object).map(|p| &p.kind);
        let kind = match key_pin {
            Some(PinKind::Unsharded | PinKind::Reference) => PinKind::Unsharded,
            Some(PinKind::ShardKey { columns }) => PinKind::Colocate {
                parent: ObjectId(k.object),
                columns: columns.clone(),
            },
            Some(PinKind::Colocate { .. }) => continue,
            None => {
                if !k.columns.iter().any(|c| c.name == KV_DK) {
                    continue;
                }
                if !add.iter().any(|p| p.object.0 == k.object) {
                    add.push(Pin {
                        object: ObjectId(k.object),
                        db: DbId(k.db),
                        kind: PinKind::ShardKey {
                            columns: vec![KV_DK.to_owned()],
                        },
                    });
                }
                PinKind::Colocate {
                    parent: ObjectId(k.object),
                    columns: vec![KV_DK.to_owned()],
                }
            }
        };
        add.push(Pin {
            object: ObjectId(e.object),
            db: DbId(e.db),
            kind,
        });
    }
    tables.retain(|t| !drop.contains(&t.object));
    located.retain(|l| !drop.contains(&l.object));
    pins.extend(add);
}

/// Which equality a fingerprint kind's joins use (chapter 22, "Comparison
/// rules per query language"), for every model whose shapes the advisor
/// reads: SQL and what compiles to it (the Analytics API, REST and gRPC)
/// use SQL's; MongoDB shapes (`$lookup`, recorded from R5) use MongoDB's.
/// Key-value, Redis, vector, search and logic fingerprints carry no join
/// edges yet (R5 and R6); `None` for them.
#[must_use]
pub fn join_rules(kind: FpKind) -> Option<Rules> {
    match kind {
        FpKind::Sql | FpKind::Analytics | FpKind::Api => Some(Rules::Sql),
        FpKind::Mongo => Some(Rules::Mongo),
        FpKind::Redis
        | FpKind::Kv
        | FpKind::Vector
        | FpKind::Search
        | FpKind::Logic
        | FpKind::Txn => None,
    }
}

fn alias_object(shape: &Shape, alias: Option<&str>) -> Option<Obj> {
    shape
        .tables
        .iter()
        .find(|t| Some(t.alias.as_str()) == alias)
        .and_then(|t| t.object_id)
        .map(|o| o.0)
}

/// One fingerprint's shape as a statement (or a transaction unit).
#[must_use]
pub fn statement(
    shape: &Shape,
    kind: FpKind,
    fingerprint: u64,
    weight: f64,
    tables: &[Table],
) -> (Option<Stmt>, Option<Unit>) {
    let known = |o: Obj| tables.iter().any(|t| t.object == o);
    if kind == FpKind::Txn {
        let writes: Vec<Obj> = shape
            .object_ids()
            .iter()
            .map(|o| o.0)
            .filter(|o| known(*o))
            .collect();
        return (
            None,
            (!writes.is_empty()).then_some(Unit { weight, writes }),
        );
    }
    let Some(rules) = join_rules(kind) else {
        return (None, None);
    };
    let mut touches: BTreeMap<Obj, Touch> = BTreeMap::new();
    for t in &shape.tables {
        if let Some(o) = t.object_id.map(|o| o.0)
            && known(o)
        {
            touches.entry(o).or_insert(Touch {
                object: o,
                ..Touch::default()
            });
        }
    }
    for p in &shape.predicates {
        if matches!(p.op, PredOp::Eq)
            && let Some(o) = alias_object(shape, p.column.table.as_deref())
            && let Some(t) = touches.get_mut(&o)
        {
            t.eq.push(p.column.col.clone());
        }
    }
    if let Some(w) = &shape.writes {
        for alias in &w.targets {
            let Some(o) = alias_object(shape, Some(alias)) else {
                continue;
            };
            let Some(t) = touches.get_mut(&o) else {
                continue;
            };
            t.write = true;
            if matches!(
                w.kind,
                WriteKind::Insert | WriteKind::Upsert | WriteKind::Replace
            ) {
                t.insert = if w.all_columns {
                    tables
                        .iter()
                        .find(|x| x.object == o)
                        .map(|x| x.columns.iter().map(|c| c.name.clone()).collect())
                        .unwrap_or_default()
                } else {
                    w.set.iter().map(|c| c.col.clone()).collect()
                };
            }
        }
    }
    let joins = shape
        .joins
        .iter()
        .filter(|j| j.op == JoinOp::Eq)
        .filter_map(|j| {
            let a = alias_object(shape, j.l.table.as_deref())?;
            let b = alias_object(shape, j.r.table.as_deref())?;
            (known(a) && known(b) && a != b).then(|| JoinEdge {
                a: (a, j.l.col.clone()),
                b: (b, j.r.col.clone()),
                rules,
            })
        })
        .collect();
    if touches.is_empty() {
        return (None, None);
    }
    (
        Some(Stmt {
            weight,
            fingerprint,
            touches: touches.into_values().collect(),
            joins,
        }),
        None,
    )
}

/// The whole input of a run for `tenant` over `window` ending at `now`,
/// read through `c` (a connection to the tenant's cluster).
///
/// # Errors
/// MariaDB's and the workload store's.
pub async fn assemble(
    c: &mut Conn,
    tenant: TenantId,
    now: SystemTime,
    window: Duration,
    split_bytes: f64,
) -> Result<Assembled> {
    let (mut tables, fks, mut located) = tables(c, tenant).await?;
    let store = WorkloadTables::for_tenant(tenant);
    let from = now.checked_sub(window).unwrap_or(SystemTime::UNIX_EPOCH);
    let fps = store
        .window(c, tenant, from, now + Duration::from_secs(3600))
        .await?;
    let mut stmts = Vec::new();
    let mut units = Vec::new();
    let mut first_seen: Option<SystemTime> = None;
    let ids: Vec<sprig_common::ids::Fingerprint> = fps.iter().map(|f| f.id).collect();
    for f in store.fingerprints(c, &ids).await? {
        first_seen = Some(first_seen.map_or(f.first_seen, |x| x.min(f.first_seen)));
    }
    for f in &fps {
        #[allow(clippy::cast_precision_loss)]
        let w = f.stats.exec_count as f64;
        let (s, u) = statement(&f.shape, f.kind, f.id.0, w, &tables);
        stmts.extend(s);
        units.extend(u);
    }
    let catalog = SchemaName::catalog(tenant).to_string();
    // Chapter 16's pins (empty before the `sharding` module's format 3).
    let mut pins = PinStore.load(c, &catalog).await?;
    kv_followers(&mut tables, &mut located, &mut pins);
    let version: Option<String> = c
        .query_first(format!(
            "SELECT CAST(value AS CHAR) FROM `{catalog}`.settings WHERE scope_kind = 'tenant' \
             AND scope_id = 0 AND setting_key = 'sprig.catalog_version'"
        ))
        .await
        .map_err(|e| db_err("catalog version", &e))?;
    Ok(Assembled {
        input: Input {
            tables,
            fks,
            stmts,
            units,
            split_bytes,
            pins,
        },
        located,
        first_seen,
        schema_version: version
            .and_then(|v| v.trim_matches('"').parse().ok())
            .unwrap_or(0),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::sharding::design::{Routing, structural};
    use crate::sharding::settings::AdvisorSettings;
    use sprig_common::ids::ObjectId;

    fn col(n: &str, class: Option<KeyClass>) -> Column {
        Column {
            name: n.into(),
            class,
            ndv: None,
            top_share: None,
            sql: None,
        }
    }

    fn kv(object: Obj, name: &str) -> Table {
        let b = Some(KeyClass::Binary);
        Table {
            object,
            db: 1,
            name: name.into(),
            columns: vec![col("k", b), col("g", Some(KeyClass::Numeric)), col("_sprig_dk", b)],
            pk: if name.starts_with("__") {
                vec!["k".into(), "g".into(), "o".into(), "e".into()]
            } else {
                vec!["k".into()]
            },
            uniques: Vec::new(),
            size_bytes: 1e6,
        }
    }

    fn at(t: &Table) -> Located {
        Located {
            object: t.object,
            db: DbId(t.db),
            schema: "t1_d1_g1_s1".into(),
            name: t.name.clone(),
        }
    }

    #[test]
    fn element_tables_follow_their_key_table() {
        let mut tables = vec![
            kv(1, "db0"),
            kv(2, "__sprig_ke_db0"),
            kv(3, "__sprig_kz_db0"),
            kv(4, "__sprig_ke_gone"),
        ];
        let mut located: Vec<Located> = tables.iter().map(at).collect();
        let mut pins = Vec::new();
        kv_followers(&mut tables, &mut located, &mut pins);
        assert_eq!(tables.len(), 3);
        assert_eq!(located.len(), 3);
        let input = Input {
            tables,
            pins,
            ..Input::default()
        };
        let (d, why) = structural(&input, &AdvisorSettings::default());
        assert_eq!(d.groups.len(), 1, "{d:?} {why:?}");
        let g = &d.groups[0];
        assert_eq!(g.key, vec![KeyClass::Binary]);
        let mut objs: Vec<Obj> = g.members.iter().map(|m| m.object).collect();
        objs.sort_unstable();
        assert_eq!(objs, vec![1, 2, 3]);
        for m in &g.members {
            assert_eq!(
                m.routing,
                Routing::Columns {
                    columns: vec!["_sprig_dk".into()]
                }
            );
        }
    }

    #[test]
    fn element_tables_of_an_unsharded_key_table_stay_unsharded() {
        let mut tables = vec![kv(1, "kvs"), kv(2, "__sprig_ke_kvs")];
        let mut located: Vec<Located> = tables.iter().map(at).collect();
        let mut pins = vec![Pin {
            object: ObjectId(1),
            db: DbId(1),
            kind: PinKind::Unsharded,
        }];
        kv_followers(&mut tables, &mut located, &mut pins);
        assert_eq!(pins.len(), 2);
        assert_eq!(pins[1].kind, PinKind::Unsharded);
        assert_eq!(kv_key_table("__sprig_kz_x"), Some("x"));
        assert_eq!(kv_key_table("__sprig_doc_x"), None);
        assert_eq!(kv_key_table("__sprig_ke_"), None);
    }
}
