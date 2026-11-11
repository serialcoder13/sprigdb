//! Chapter 33's key-value tables in the advisor (decisions R28, R6 rounds
//! 4 and 5).
//!
//! A Redis database or key-value namespace is one **unit**: its key table
//! (`db0`, `cache`) and its element tables (`__sprig_ke_<t>`,
//! `__sprig_kz_<t>`). Clients can't pin `__sprig_` tables, so the advisor
//! places a unit as a whole, through implied pins:
//!
//! - **The key table is pinned by the client.** `UNSHARDED` or `REFERENCE`
//!   keeps the element tables unsharded; `SHARD KEY (cols)` makes them
//!   `COLOCATE WITH` it on `cols`.
//! - **No pin.** The unit is sharded only when one of the rules that shard
//!   anything else holds (round 5, the lead's change to R28):
//!   - **`min_shards`:** its database's `sharding.min_shards` is above 1;
//!   - **size:** its tables hold at least half of `sharding.split_size_gb`,
//!     the advisor's unit for an expected shard;
//!   - **load:** its database's shard is write-hot by chapter 20's rule
//!     (`sharding.split_write_share_pct` of its cluster's writes for
//!     `sharding.split_sustain_minutes`, on a hot cluster) and the unit
//!     takes at least that share of the database's recorded writes.
//!
//!   Then its distribution key `_sprig_dk` must still pass the key quality
//!   filters (distinct values and the top value's share, sampled by
//!   [`super::input::assemble`]). A unit that passes gets `SHARD KEY
//!   (_sprig_dk)` and its element tables `COLOCATE WITH` the key table on
//!   `_sprig_dk`, so hash-tagged keys and their elements stay on one shard
//!   as Redis Cluster keeps them in one slot. Any other unit stays
//!   unsharded, key and element tables alike.
//!
//! Every decision is a line of the run's evidence.

use sprig_common::ids::{DbId, ObjectId};
use sprig_shard::KeyClass;
use sprig_shard::pins::{Pin, PinKind};

use super::design::{Column, Input, Obj, Table, key_quality};
use super::settings::AdvisorSettings;

/// The distribution-key column of chapter 33's tables.
pub const KV_DK: &str = "_sprig_dk";

/// One Redis database or key-value namespace.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct KvUnit {
    /// The key table.
    pub key: Obj,
    /// Its database.
    pub db: u32,
    /// Its element tables.
    pub elements: Vec<Obj>,
    /// Data and index bytes of the key and element tables.
    pub bytes: f64,
    /// Distinct `_sprig_dk` values (sampled), if known.
    pub dk_ndv: Option<f64>,
    /// The share of keys holding the most common `_sprig_dk`, if known.
    pub dk_top_share: Option<f64>,
    /// The unit's share of its database's recorded write executions.
    pub write_share: f64,
    /// `sharding.min_shards` of its database (0 or 1: none asked).
    pub min_shards: u32,
    /// Its database's shard is write-hot by chapter 20's load rule.
    pub write_hot: bool,
}

/// Human-readable bytes for the evidence.
#[must_use]
pub fn bytes_text(b: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b.max(0.0);
    let mut i = 0;
    while v >= 1024.0 && i + 1 < UNITS.len() {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{v:.0} {}", UNITS[i])
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

/// Why the unit's own rules say to shard it; empty when none does.
#[must_use]
pub fn reasons(u: &KvUnit, split_bytes: f64, split_write_share: f64) -> Vec<String> {
    let mut why = Vec::new();
    if u.min_shards > 1 {
        why.push(format!("min_shards {}", u.min_shards));
    }
    let half = split_bytes / 2.0;
    if split_bytes > 0.0 && u.bytes >= half {
        why.push(format!(
            "{} of data, at least half the split size ({})",
            bytes_text(u.bytes),
            bytes_text(half)
        ));
    }
    let share = if split_write_share > 0.0 {
        split_write_share
    } else {
        0.5
    };
    if u.write_hot && u.write_share >= share {
        why.push(format!(
            "its database's shard is write-hot and it takes {:.0}% of the writes",
            u.write_share * 100.0
        ));
    }
    why
}

/// Applies the unit rule (module docs) to `input`: adds the implied pins
/// of every unit in `input.kv` and their evidence to `input.notes`, and
/// empties `input.kv`, so a second call changes nothing.
pub fn imply(input: &mut Input, s: &AdvisorSettings) {
    let units = std::mem::take(&mut input.kv);
    let mut add: Vec<Pin> = Vec::new();
    for u in units {
        let Some(k) = input.tables.iter().find(|t| t.object == u.key).cloned() else {
            continue;
        };
        let pin = |o: Obj, kind: PinKind| Pin {
            object: ObjectId(o),
            db: DbId(u.db),
            kind,
        };
        let colocate = |columns: Vec<String>| PinKind::Colocate {
            parent: ObjectId(k.object),
            columns,
        };
        let elements = match input.pin(k.object).cloned() {
            Some(PinKind::Unsharded | PinKind::Reference) => Some(PinKind::Unsharded),
            Some(PinKind::ShardKey { columns }) => Some(colocate(columns)),
            Some(PinKind::Colocate { .. }) => None,
            None => {
                if !k.columns.iter().any(|c| c.name == KV_DK) {
                    continue;
                }
                let why = reasons(&u, input.split_bytes, input.split_write_share);
                let half = bytes_text(input.split_bytes / 2.0);
                let kind = if why.is_empty() {
                    input.notes.push(format!(
                        "{}: key-value data stays unsharded ({}, below half the split size \
                         {half}; min_shards {}; not write-hot)",
                        k.name,
                        bytes_text(u.bytes),
                        u.min_shards.max(1)
                    ));
                    None
                } else {
                    let probe = Table {
                        columns: vec![Column {
                            name: KV_DK.to_owned(),
                            class: Some(KeyClass::Binary),
                            ndv: u.dk_ndv,
                            top_share: u.dk_top_share,
                            sql: None,
                        }],
                        ..k.clone()
                    };
                    match key_quality(&probe, &[KV_DK.to_owned()], u.bytes, input, s) {
                        Ok(_) => {
                            input.notes.push(format!(
                                "{}: key-value data sharded on {KV_DK} ({})",
                                k.name,
                                why.join("; ")
                            ));
                            Some(())
                        }
                        Err(e) => {
                            input.notes.push(format!(
                                "{}: key-value data stays unsharded ({e}), although {}",
                                k.name,
                                why.join("; ")
                            ));
                            None
                        }
                    }
                };
                input.kv_decided.push(k.object);
                input.kv_decided.extend(u.elements.iter().copied());
                if kind.is_some() {
                    add.push(pin(
                        k.object,
                        PinKind::ShardKey {
                            columns: vec![KV_DK.to_owned()],
                        },
                    ));
                    Some(colocate(vec![KV_DK.to_owned()]))
                } else {
                    add.push(pin(k.object, PinKind::Unsharded));
                    Some(PinKind::Unsharded)
                }
            }
        };
        let Some(kind) = elements else { continue };
        for e in &u.elements {
            if input.pin(*e).is_none() && !add.iter().any(|p| p.object.0 == *e) {
                add.push(pin(*e, kind.clone()));
            }
        }
    }
    input.pins.extend(add);
    input.pins.sort_by_key(|p| p.object);
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::sharding::design::{Routing, search, structural};

    const GB: f64 = 1024.0 * 1024.0 * 1024.0;

    fn col(n: &str, class: Option<KeyClass>) -> Column {
        Column {
            name: n.into(),
            class,
            ndv: None,
            top_share: None,
            sql: None,
        }
    }

    fn kv(object: Obj, name: &str, bytes: f64) -> Table {
        let b = Some(KeyClass::Binary);
        Table {
            object,
            db: 1,
            name: name.into(),
            columns: vec![
                col("k", b),
                col("g", Some(KeyClass::Numeric)),
                col(KV_DK, b),
            ],
            pk: if name.starts_with("__") {
                vec!["k".into(), "g".into(), "o".into(), "e".into()]
            } else {
                vec!["k".into()]
            },
            uniques: Vec::new(),
            size_bytes: bytes,
        }
    }

    /// A Redis database `db0` (objects 1–3) of `bytes` in all, with
    /// `ndv` distinct hash tags, beside a SQL table `orders` (object 9).
    fn redis(bytes: f64, ndv: f64) -> Input {
        let n = Some(KeyClass::Numeric);
        let mut orders = Table {
            object: 9,
            db: 1,
            name: "orders".into(),
            columns: vec![col("id", n)],
            pk: vec!["id".into()],
            uniques: Vec::new(),
            size_bytes: 40.0 * GB,
        };
        orders.columns[0].ndv = Some(1e7);
        Input {
            tables: vec![
                kv(1, "db0", bytes / 2.0),
                kv(2, "__sprig_ke_db0", bytes / 4.0),
                kv(3, "__sprig_kz_db0", bytes / 4.0),
                orders,
            ],
            split_bytes: 32.0 * GB,
            kv: vec![KvUnit {
                key: 1,
                db: 1,
                elements: vec![2, 3],
                bytes,
                dk_ndv: Some(ndv),
                dk_top_share: Some(0.01),
                ..KvUnit::default()
            }],
            ..Input::default()
        }
    }

    fn placed(input: &Input) -> (Vec<Obj>, Vec<Obj>, Vec<String>) {
        let (d, why) = structural(input, &AdvisorSettings::default());
        let mut sharded: Vec<Obj> = d
            .groups
            .iter()
            .flat_map(|g| g.members.iter().map(|m| m.object))
            .collect();
        sharded.sort_unstable();
        let mut out = d.unsharded.clone();
        out.sort_unstable();
        (sharded, out, why)
    }

    #[test]
    fn a_small_redis_database_stays_unsharded() {
        let input = redis(64.0 * 1024.0 * 1024.0, 5_000.0);
        let (sharded, out, why) = placed(&input);
        assert_eq!(sharded, vec![9], "{why:?}");
        assert_eq!(out, vec![1, 2, 3]);
        assert!(
            why.iter()
                .any(|w| w.starts_with("db0: key-value data stays unsharded (64.0 MiB")),
            "{why:?}"
        );
        assert!(
            !why.iter().any(|w| w.contains("pinned UNSHARDED")),
            "{why:?}"
        );
        // The search keeps it there: the pins hold.
        let r = search(&input, &AdvisorSettings::default());
        assert!(
            r.best
                .groups
                .iter()
                .all(|g| g.members.iter().all(|m| m.object == 9)),
            "{:?}",
            r.best
        );
    }

    #[test]
    fn a_big_redis_database_is_sharded_with_its_element_tables() {
        let input = redis(20.0 * GB, 1e6);
        let (d, why) = structural(&input, &AdvisorSettings::default());
        let g = d
            .groups
            .iter()
            .find(|g| g.members.iter().any(|m| m.object == 1))
            .unwrap();
        let mut objs: Vec<Obj> = g.members.iter().map(|m| m.object).collect();
        objs.sort_unstable();
        assert_eq!(objs, vec![1, 2, 3], "{why:?}");
        for m in &g.members {
            assert_eq!(
                m.routing,
                Routing::Columns {
                    columns: vec![KV_DK.into()]
                }
            );
        }
        assert!(
            why.iter().any(|w| w
                == "db0: key-value data sharded on _sprig_dk (20.0 GiB of data, at least half \
                    the split size (16.0 GiB))"),
            "{why:?}"
        );
    }

    #[test]
    fn min_shards_shards_a_small_unit_and_quality_can_still_refuse() {
        let mut input = redis(1e6, 5_000.0);
        input.kv[0].min_shards = 4;
        let (sharded, _, why) = placed(&input);
        assert_eq!(sharded, vec![1, 2, 3, 9], "{why:?}");
        // One hash tag holds most keys: the skew filter refuses the key.
        let mut skewed = redis(1e6, 5_000.0);
        skewed.kv[0].min_shards = 4;
        skewed.kv[0].dk_top_share = Some(0.6);
        let (sharded, out, why) = placed(&skewed);
        assert_eq!(sharded, vec![9]);
        assert_eq!(out, vec![1, 2, 3]);
        assert!(
            why.iter().any(|w| w.contains("one value holds 60% of rows")
                && w.contains("although min_shards 4")),
            "{why:?}"
        );
        // Too few distinct tags for the expected shards.
        let mut few = redis(1e6, 50.0);
        few.kv[0].min_shards = 4;
        let (sharded, _, why) = placed(&few);
        assert_eq!(sharded, vec![9], "{why:?}");
    }

    #[test]
    fn load_shards_a_unit_only_when_its_shard_is_hot_and_it_takes_the_writes() {
        let mut input = redis(1e6, 5_000.0);
        input.split_write_share = 0.5;
        input.kv[0].write_share = 0.8;
        assert_eq!(placed(&input).0, vec![9], "not hot");
        input.kv[0].write_hot = true;
        assert_eq!(placed(&input).0, vec![1, 2, 3, 9]);
        input.kv[0].write_share = 0.3;
        input.kv[0].write_hot = true;
        assert_eq!(placed(&input).0, vec![9], "hot, but others write");
    }

    #[test]
    fn client_pins_on_the_key_table_still_lead() {
        let mut input = redis(20.0 * GB, 1e6);
        input.pins.push(Pin {
            object: ObjectId(1),
            db: DbId(1),
            kind: PinKind::Unsharded,
        });
        let (sharded, out, _) = placed(&input);
        assert_eq!(sharded, vec![9]);
        assert_eq!(out, vec![1, 2, 3]);
        // A small unit the client pinned SHARD KEY is sharded anyway.
        let mut small = redis(1e6, 50.0);
        small.pins.push(Pin {
            object: ObjectId(1),
            db: DbId(1),
            kind: PinKind::ShardKey {
                columns: vec![KV_DK.into()],
            },
        });
        assert_eq!(placed(&small).0, vec![1, 2, 3, 9]);
    }

    #[test]
    fn imply_is_idempotent() {
        let mut input = redis(1e6, 5_000.0);
        imply(&mut input, &AdvisorSettings::default());
        let once = input.clone();
        imply(&mut input, &AdvisorSettings::default());
        assert_eq!(once, input);
        assert_eq!(input.pins.len(), 3);
        assert_eq!(bytes_text(1536.0), "1.5 KiB");
    }
}
