//! Hash-range routing for split, merge and shard-move jobs (chapter 10,
//! "Filtering and routing"; chapter 20).
//!
//! A job whose spec routes by hash range (`Routing::HashRange`) reads one
//! or more shard schemas of a table group on one cluster and writes new
//! shard schemas, with new shard ids, on one cluster:
//!
//! - a **split** reads shard `s` once and writes each row to the child whose
//!   range holds the row's `_sprig_rk`;
//! - a **merge** reads two or more adjacent shards and writes one shard
//!   covering their ranges;
//! - a **shard move** reads one shard and writes one shard, with a new id,
//!   on another cluster.
//!
//! Every member table carries `_sprig_rk` (chapter 15), so routing reads one
//! `BIGINT UNSIGNED` from the row image: the mover never normalizes keys.
//! [`ShardRoute::route_ops`] turns the engine's apply operations on source
//! tables into operations on target tables. An upsert also deletes its keys
//! from the sibling targets, in the same transaction: a row whose
//! `_sprig_rk` changed (a key change inside the source's range) leaves the
//! child it was in. A delete goes to every target. So, at every applied
//! position, each key is in at most one target, the one its current row
//! routes to (the engine's invariant, per target).
//!
//! [`ShardRoute::merge_digests`] turns the targets' digests of one source
//! key range into what the source's digests should equal: rows of the
//! source's range only (a merge target also holds the other sources'
//! rows), with a row in the wrong target, or in two, made to mismatch so
//! verification repairs it.

use std::collections::BTreeMap;
use std::sync::Arc;

use sprig_common::ids::{GroupId, ShardId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};

use crate::api::{HashRange, JobSpec, Routing};
use crate::model::{ApplyOp, KeyBytes, Row, RowDigest, TableMeta, Value};

/// The routing column every member of a sharded group carries.
pub const RK_COLUMN: &str = "_sprig_rk";

impl HashRange {
    /// Whether `h` is in `[lo, hi)`.
    #[must_use]
    pub fn contains(&self, h: u64) -> bool {
        h >= self.lo && self.hi.is_none_or(|hi| h < hi)
    }
}

/// One target schema of a source schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetRoute {
    /// The target schema (`t<tenant>_d<db>_g<group>_s<new shard>`).
    pub schema: String,
    /// The new shard.
    pub shard: u32,
    /// Its range.
    pub range: HashRange,
}

/// One source schema and where its rows go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRoute {
    /// The source schema.
    pub schema: String,
    /// The source shard.
    pub shard: u32,
    /// The source shard's range.
    pub range: HashRange,
    /// The targets of the same database whose ranges meet the source's,
    /// in range order.
    pub targets: Vec<TargetRoute>,
}

/// How a hash-range job maps source schemas to target schemas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRoute {
    /// The table group.
    pub group: u32,
    /// Per source schema.
    pub sources: Vec<SourceRoute>,
}

fn unsupported(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::MoveUnsupported, m)
}

fn overlaps(a: &HashRange, b: &HashRange) -> bool {
    let a_hi = a.hi.map_or(u128::from(u64::MAX) + 1, u128::from);
    let b_hi = b.hi.map_or(u128::from(u64::MAX) + 1, u128::from);
    u128::from(a.lo) < b_hi && u128::from(b.lo) < a_hi
}

impl ShardRoute {
    /// The route of a hash-range job; `None` for a job that copies whole
    /// schemas.
    ///
    /// # Errors
    /// 39136 (`MoveUnsupported`) for a spec whose schemas aren't shard
    /// schemas of the job's group, or without ranges.
    pub fn from_spec(spec: &JobSpec) -> Result<Option<Self>> {
        let Routing::HashRange { group } = spec.routing else {
            return Ok(None);
        };
        let mut sources = Vec::new();
        for s in &spec.sources {
            let range = s
                .range
                .ok_or_else(|| unsupported("a hash-range source needs its range"))?;
            if s.schemas.is_empty() {
                return Err(unsupported("a hash-range source names its schemas"));
            }
            for schema in &s.schemas {
                let Ok(SchemaName::Shard {
                    tenant,
                    db,
                    group: g,
                    shard,
                }) = schema.parse::<SchemaName>()
                else {
                    return Err(unsupported(format!("{schema} isn't a shard schema")));
                };
                if g != GroupId(group) || tenant != spec.tenant || shard != ShardId(s.shard) {
                    return Err(unsupported(format!(
                        "{schema} isn't shard {} of group {group}",
                        s.shard
                    )));
                }
                let mut targets = Vec::new();
                for t in &spec.targets {
                    let tr = t
                        .range
                        .ok_or_else(|| unsupported("a hash-range target needs its range"))?;
                    if overlaps(&range, &tr) {
                        targets.push(TargetRoute {
                            schema: SchemaName::shard(tenant, db, g, ShardId(t.shard)).to_string(),
                            shard: t.shard,
                            range: tr,
                        });
                    }
                }
                if targets.is_empty() {
                    return Err(unsupported(format!("no target covers {schema}")));
                }
                targets.sort_by_key(|t| t.range.lo);
                sources.push(SourceRoute {
                    schema: schema.clone(),
                    shard: s.shard,
                    range,
                    targets,
                });
            }
        }
        Ok(Some(Self { group, sources }))
    }

    /// The source schema `schema`'s route.
    #[must_use]
    pub fn source(&self, schema: &str) -> Option<&SourceRoute> {
        self.sources.iter().find(|s| s.schema == schema)
    }

    /// Every target schema, sorted, once.
    #[must_use]
    pub fn target_schemas(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .sources
            .iter()
            .flat_map(|s| s.targets.iter().map(|t| t.schema.clone()))
            .collect();
        v.sort();
        v.dedup();
        v
    }

    /// The target schemas of `source` (empty for an unknown schema).
    #[must_use]
    pub fn targets_of(&self, source: &str) -> Vec<String> {
        self.source(source)
            .map(|s| s.targets.iter().map(|t| t.schema.clone()).collect())
            .unwrap_or_default()
    }

    /// Rewrites the source's object statements (schema creation, then
    /// `USE` segments of views, routines or triggers, as `SourceIo::objects`
    /// and `SourceIo::triggers` list them) for the targets: each segment of
    /// a routed source schema is repeated for each of its targets, with the
    /// schema's quoted name replaced; segments of other schemas are
    /// dropped.
    #[must_use]
    pub fn rewrite_statements(&self, stmts: &[String]) -> Vec<String> {
        let mut out = Vec::new();
        let mut segment: Option<(&SourceRoute, Vec<&String>)> = None;
        let flush = |seg: &mut Option<(&SourceRoute, Vec<&String>)>, out: &mut Vec<String>| {
            if let Some((src, body)) = seg.take() {
                for t in &src.targets {
                    out.push(format!("USE `{}`", t.schema));
                    for q in &body {
                        out.push(
                            q.replace(&format!("`{}`", src.schema), &format!("`{}`", t.schema)),
                        );
                    }
                }
            }
        };
        let mut skipping = false;
        for q in stmts {
            if let Some(rest) = q.strip_prefix("USE ") {
                flush(&mut segment, &mut out);
                let name = rest.trim().trim_matches('`');
                match self.source(name) {
                    Some(src) => {
                        segment = Some((src, Vec::new()));
                        skipping = false;
                    }
                    None => skipping = true,
                }
                continue;
            }
            if q.starts_with("CREATE DATABASE") {
                for src in &self.sources {
                    if q.contains(&format!("`{}`", src.schema)) {
                        for t in &src.targets {
                            out.push(
                                q.replace(&format!("`{}`", src.schema), &format!("`{}`", t.schema)),
                            );
                        }
                    }
                }
                continue;
            }
            if skipping {
                continue;
            }
            // A statement before any USE has nothing to attach it to.
            if let Some((_, body)) = &mut segment {
                body.push(q);
            }
        }
        flush(&mut segment, &mut out);
        // A merge's sources share their target: create it once.
        let mut seen = std::collections::HashSet::new();
        out.retain(|q| !q.starts_with("CREATE DATABASE") || seen.insert(q.clone()));
        out
    }

    /// The index of `_sprig_rk` in `t`'s columns.
    #[must_use]
    pub fn rk_column(t: &TableMeta) -> Option<usize> {
        t.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(RK_COLUMN))
    }

    /// The routing hash of `row` of `t`.
    ///
    /// # Errors
    /// 39136 for a table without `_sprig_rk` or a row whose `_sprig_rk` is
    /// `NULL` (its back-fill isn't done).
    pub fn rk_of(t: &TableMeta, row: &[Value]) -> Result<u64> {
        let i = Self::rk_column(t).ok_or_else(|| {
            unsupported(format!(
                "{}.{} has no {RK_COLUMN}: it can't be split",
                t.schema, t.name
            ))
        })?;
        match row.get(i) {
            Some(Value::UInt(u)) => Ok(*u),
            Some(Value::Int(i)) => Ok(u64::from_ne_bytes(i.to_ne_bytes())),
            Some(Value::Bytes(b)) => std::str::from_utf8(b)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    unsupported(format!("{}.{}: unreadable {RK_COLUMN}", t.schema, t.name))
                }),
            _ => Err(unsupported(format!(
                "{}.{} has a row without {RK_COLUMN} (back-fill it first)",
                t.schema, t.name
            ))),
        }
    }

    /// `t` as it is named in `schema`.
    #[must_use]
    pub fn retarget(t: &TableMeta, schema: &str) -> Arc<TableMeta> {
        let mut x = t.clone();
        schema.clone_into(&mut x.schema);
        Arc::new(x)
    }

    /// The engine's operations on source tables as operations on target
    /// tables, in an order that keeps each key in at most one target.
    ///
    /// # Errors
    /// 39136 for a table of an unknown schema, a row without
    /// `_sprig_rk`, or a row outside its source shard's range.
    pub fn route_ops(&self, ops: &[ApplyOp]) -> Result<Vec<ApplyOp>> {
        let mut out = Vec::with_capacity(ops.len() * 2);
        for op in ops {
            match op {
                ApplyOp::Upsert { table, rows } => {
                    let src = self.source(&table.schema).ok_or_else(|| {
                        unsupported(format!("{} isn't a source of this job", table.schema))
                    })?;
                    let mut per: Vec<Vec<Row>> = vec![Vec::new(); src.targets.len()];
                    for r in rows {
                        let h = Self::rk_of(table, r)?;
                        let i = src
                            .targets
                            .iter()
                            .position(|t| t.range.contains(h))
                            .ok_or_else(|| {
                                unsupported(format!(
                                    "{}.{}: a row's {RK_COLUMN} {h:#018x} is outside shard {}'s \
                                     range",
                                    table.schema, table.name, src.shard
                                ))
                            })?;
                        per[i].push(r.clone());
                    }
                    if src.targets.len() > 1 {
                        for (j, t) in src.targets.iter().enumerate() {
                            let keys: Vec<Row> = per
                                .iter()
                                .enumerate()
                                .filter(|(i, _)| *i != j)
                                .flat_map(|(_, rs)| rs.iter().map(|r| table.key_values(r)))
                                .collect();
                            if !keys.is_empty() {
                                out.push(ApplyOp::Delete {
                                    table: Self::retarget(table, &t.schema),
                                    keys,
                                });
                            }
                        }
                    }
                    for (t, rows) in src.targets.iter().zip(per) {
                        if !rows.is_empty() {
                            out.push(ApplyOp::Upsert {
                                table: Self::retarget(table, &t.schema),
                                rows,
                            });
                        }
                    }
                }
                ApplyOp::Delete { table, keys } => {
                    let src = self.source(&table.schema).ok_or_else(|| {
                        unsupported(format!("{} isn't a source of this job", table.schema))
                    })?;
                    for t in &src.targets {
                        out.push(ApplyOp::Delete {
                            table: Self::retarget(table, &t.schema),
                            keys: keys.clone(),
                        });
                    }
                }
            }
        }
        Ok(out)
    }

    /// What the targets hold of source table `t`'s key range: `digests`
    /// are `(target schema, digest, _sprig_rk)` rows read from every target
    /// of `t`'s schema over the range. Rows outside the source's range are
    /// left out (another source's, in a merge); a row in a target whose
    /// range doesn't hold its hash, or in two targets, gets a digest that
    /// can't match, so verification repairs it.
    #[must_use]
    pub fn merge_digests(
        &self,
        t: &TableMeta,
        digests: Vec<(String, RowDigest, Option<u64>)>,
    ) -> Vec<RowDigest> {
        let Some(src) = self.source(&t.schema) else {
            return Vec::new();
        };
        let mut out: BTreeMap<KeyBytes, RowDigest> = BTreeMap::new();
        for (schema, mut d, rk) in digests {
            let Some(target) = src.targets.iter().find(|x| x.schema == schema) else {
                continue;
            };
            match rk {
                Some(h) if !src.range.contains(h) => continue,
                Some(h) if target.range.contains(h) => {}
                _ => d.hash = !d.hash,
            }
            if let Some(prev) = out.get_mut(&d.key) {
                prev.hash = !prev.hash ^ 0x5bd1_e995;
                continue;
            }
            out.insert(d.key.clone(), d);
        }
        out.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_common::ids::{ClusterId, TenantId};

    use super::*;
    use crate::api::{CutoverMode, JobKind, SourceSpec, TargetSpec, VerifyMode};
    use crate::model::ColumnMeta;

    const HALF: u64 = 1 << 63;

    fn spec(sources: Vec<(u32, HashRange)>, targets: Vec<(u32, HashRange)>) -> JobSpec {
        let c = ClusterId::new("dc-a-c1").unwrap();
        JobSpec {
            kind: JobKind::SplitShard,
            tenant: TenantId(42),
            sources: sources
                .into_iter()
                .map(|(s, r)| SourceSpec {
                    cluster: c.clone(),
                    shard: s,
                    schemas: vec![format!("t42_d1_g3_s{s}"), format!("t42_d2_g3_s{s}")],
                    tables: None,
                    range: Some(r),
                })
                .collect(),
            targets: targets
                .into_iter()
                .map(|(s, r)| TargetSpec {
                    cluster: c.clone(),
                    shard: s,
                    schemas: Vec::new(),
                    range: Some(r),
                    ddl: None,
                })
                .collect(),
            routing: Routing::HashRange { group: 3 },
            cutover: CutoverMode::Auto,
            verify: VerifyMode::Full,
            decision_id: None,
            requested_by: None,
            max_hold_ms: None,
            retain_ms: None,
        }
    }

    fn table(schema: &str) -> TableMeta {
        let col = |n: &str| ColumnMeta {
            name: n.into(),
            generated: false,
            numeric: false,
        };
        TableMeta {
            schema: schema.into(),
            name: "orders".into(),
            columns: vec![col("id"), col("v"), col("_sprig_rk")],
            pk: vec![0],
            create_sql: String::new(),
        }
    }

    fn row(id: i64, rk: u64) -> Row {
        vec![Value::Int(id), Value::Bytes(b"x".to_vec()), Value::UInt(rk)]
    }

    #[test]
    fn a_split_routes_by_rk_and_clears_siblings() {
        let r = ShardRoute::from_spec(&spec(
            vec![(4, HashRange { lo: 0, hi: None })],
            vec![
                (
                    6,
                    HashRange {
                        lo: 0,
                        hi: Some(HALF),
                    },
                ),
                (7, HashRange { lo: HALF, hi: None }),
            ],
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            r.target_schemas(),
            vec![
                "t42_d1_g3_s6",
                "t42_d1_g3_s7",
                "t42_d2_g3_s6",
                "t42_d2_g3_s7"
            ]
        );
        let t = Arc::new(table("t42_d1_g3_s4"));
        let ops = r
            .route_ops(&[ApplyOp::Upsert {
                table: t.clone(),
                rows: vec![row(1, 5), row(2, HALF + 1), row(3, HALF - 1)],
            }])
            .unwrap();
        // Deletes of the other child's keys first, then the upserts.
        let desc: Vec<(String, bool, usize)> = ops
            .iter()
            .map(|o| match o {
                ApplyOp::Upsert { table, rows } => (table.schema.clone(), true, rows.len()),
                ApplyOp::Delete { table, keys } => (table.schema.clone(), false, keys.len()),
            })
            .collect();
        assert_eq!(
            desc,
            vec![
                ("t42_d1_g3_s6".into(), false, 1),
                ("t42_d1_g3_s7".into(), false, 2),
                ("t42_d1_g3_s6".into(), true, 2),
                ("t42_d1_g3_s7".into(), true, 1),
            ]
        );
        let dels = r
            .route_ops(&[ApplyOp::Delete {
                table: t.clone(),
                keys: vec![vec![Value::Int(9)]],
            }])
            .unwrap();
        assert_eq!(dels.len(), 2);
        // A row without its hash fails the job instead of guessing.
        let mut bad = row(4, 0);
        bad[2] = Value::NULL;
        assert!(
            r.route_ops(&[ApplyOp::Upsert {
                table: t,
                rows: vec![bad],
            }])
            .is_err()
        );
    }

    #[test]
    fn a_merge_keeps_each_sources_rows_apart_in_verification() {
        let r = ShardRoute::from_spec(&spec(
            vec![
                (
                    4,
                    HashRange {
                        lo: 0,
                        hi: Some(HALF),
                    },
                ),
                (5, HashRange { lo: HALF, hi: None }),
            ],
            vec![(8, HashRange { lo: 0, hi: None })],
        ))
        .unwrap()
        .unwrap();
        assert_eq!(r.target_schemas(), vec!["t42_d1_g3_s8", "t42_d2_g3_s8"]);
        let t = table("t42_d1_g3_s4");
        let d = |id: i64, h: u64| RowDigest {
            key: TableMeta::key_of_values(&[Value::Int(id)]),
            key_values: vec![Value::Int(id)],
            hash: h,
        };
        let got = r.merge_digests(
            &t,
            vec![
                ("t42_d1_g3_s8".into(), d(1, 10), Some(5)),
                ("t42_d1_g3_s8".into(), d(2, 20), Some(HALF + 3)), // shard 5's
            ],
        );
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].hash, 10);
    }

    #[test]
    fn misplaced_and_doubled_rows_fail_verification() {
        let r = ShardRoute::from_spec(&spec(
            vec![(4, HashRange { lo: 0, hi: None })],
            vec![
                (
                    6,
                    HashRange {
                        lo: 0,
                        hi: Some(HALF),
                    },
                ),
                (7, HashRange { lo: HALF, hi: None }),
            ],
        ))
        .unwrap()
        .unwrap();
        let t = table("t42_d1_g3_s4");
        let d = |id: i64, h: u64| RowDigest {
            key: TableMeta::key_of_values(&[Value::Int(id)]),
            key_values: vec![Value::Int(id)],
            hash: h,
        };
        let got = r.merge_digests(
            &t,
            vec![
                ("t42_d1_g3_s7".into(), d(1, 10), Some(5)), // belongs in s6
                ("t42_d1_g3_s6".into(), d(2, 20), Some(6)),
                ("t42_d1_g3_s7".into(), d(2, 20), Some(6)), // twice
            ],
        );
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|x| x.hash != 10 && x.hash != 20));
    }

    #[test]
    fn object_statements_are_repeated_per_target() {
        let r = ShardRoute::from_spec(&spec(
            vec![(4, HashRange { lo: 0, hi: None })],
            vec![
                (
                    6,
                    HashRange {
                        lo: 0,
                        hi: Some(HALF),
                    },
                ),
                (7, HashRange { lo: HALF, hi: None }),
            ],
        ))
        .unwrap()
        .unwrap();
        let got = r.rewrite_statements(&[
            "CREATE DATABASE IF NOT EXISTS `t42_d1_g3_s4` CHARACTER SET utf8mb4".into(),
            "USE `t42_d1_g3_s4`".into(),
            "CREATE VIEW v AS SELECT * FROM `t42_d1_g3_s4`.orders".into(),
            "USE `t42_sprig`".into(),
            "CREATE VIEW nope AS SELECT 1".into(),
        ]);
        assert_eq!(
            got,
            vec![
                "CREATE DATABASE IF NOT EXISTS `t42_d1_g3_s6` CHARACTER SET utf8mb4",
                "CREATE DATABASE IF NOT EXISTS `t42_d1_g3_s7` CHARACTER SET utf8mb4",
                "USE `t42_d1_g3_s6`",
                "CREATE VIEW v AS SELECT * FROM `t42_d1_g3_s6`.orders",
                "USE `t42_d1_g3_s7`",
                "CREATE VIEW v AS SELECT * FROM `t42_d1_g3_s7`.orders",
            ]
        );
    }

    #[test]
    fn specs_must_name_shards_of_the_group() {
        let mut s = spec(
            vec![(4, HashRange { lo: 0, hi: None })],
            vec![(6, HashRange { lo: 0, hi: None })],
        );
        s.sources[0].schemas = vec!["t42_d1_g9_s4".into()];
        assert!(ShardRoute::from_spec(&s).is_err());
        s.sources[0].schemas = vec!["t42_d1_g3_s4".into()];
        s.sources[0].range = None;
        assert!(ShardRoute::from_spec(&s).is_err());
    }
}
