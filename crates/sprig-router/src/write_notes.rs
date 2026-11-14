//! The routing hashes a plan used, for the split advisor's write sketches.
//!
//! While [`capture`] runs a planning call, every key value the planner
//! routes ([`crate::topology::Group::shard_of`]) is noted with its group,
//! shard, range and hash. The gateway captures around the planning of
//! `INSERT`, `REPLACE`, `UPDATE` and `DELETE` and counts the notes into its
//! sketches; outside a capture noting costs one thread-local check.
//! Planning is synchronous, so the notes of one call stay on its thread.

use std::cell::RefCell;

/// One routed key value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    /// The table group.
    pub group: u32,
    /// The shard id.
    pub shard: u32,
    /// The shard's cluster.
    pub cluster: String,
    /// The shard's first hash.
    pub lo: u64,
    /// One past its last; `None` is 2^64.
    pub hi: Option<u64>,
    /// The value's routing hash.
    pub hash: u64,
}

/// The most notes one capture keeps (a huge multi-row `INSERT` counts its
/// first rows by hash).
pub const MAX_NOTES: usize = 100_000;

thread_local! {
    static NOTES: RefCell<Option<Vec<Note>>> = const { RefCell::new(None) };
}

/// Runs `f`, returning what it returned and the key values it routed.
pub fn capture<T>(f: impl FnOnce() -> T) -> (T, Vec<Note>) {
    let outer = NOTES.with(|n| n.borrow_mut().replace(Vec::new()));
    let out = f();
    let notes = NOTES.with(|n| std::mem::replace(&mut *n.borrow_mut(), outer));
    (out, notes.unwrap_or_default())
}

/// Notes that `g` routed `hash` to its shard `i` (inside a capture).
pub(crate) fn note(g: &crate::topology::Group, i: usize, hash: u64) {
    NOTES.with(|n| {
        let mut n = n.borrow_mut();
        let Some(v) = n.as_mut() else { return };
        let Some(s) = g.shards.get(i) else { return };
        if v.len() < MAX_NOTES {
            v.push(Note {
                group: g.id,
                shard: s.shard,
                cluster: s.cluster.clone(),
                lo: s.lo,
                hi: g.shards.get(i + 1).map(|x| x.lo),
                hash,
            });
        }
    });
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::topology::{Group, KeyClass, KeyValue, ShardLoc};

    fn group() -> Group {
        let loc = |shard: u32, lo: u64, cluster: &str| ShardLoc {
            shard,
            schema: format!("t1_d1_g2_s{shard}"),
            cluster: cluster.to_owned(),
            lo,
            epoch: 1,
            held_until_ms: None,
        };
        Group {
            id: 2,
            db: 1,
            version: 1,
            key_class: KeyClass::Int,
            spec: sprig_shard::KeySpec::new(vec![sprig_shard::KeyClass::Numeric]).unwrap(),
            shards: vec![loc(4, 0, "c1"), loc(5, 1 << 63, "c2")],
        }
    }

    #[test]
    fn notes_are_kept_only_inside_a_capture() {
        let g = group();
        let _ = g.shard_of(&KeyValue::Int(42));
        let (i, notes) = capture(|| g.shard_of(&KeyValue::Int(42)).unwrap());
        assert_eq!(notes.len(), 1);
        let n = &notes[0];
        assert_eq!(n.hash, g.hash_of(&KeyValue::Int(42)).unwrap());
        assert_eq!((n.group, n.shard), (2, g.shards[i].shard));
        let hi = if i == 0 { Some(1 << 63) } else { None };
        assert_eq!((n.lo, n.hi), (g.shards[i].lo, hi));
        assert_eq!(n.cluster, g.shards[i].cluster);
        // Nested captures keep their own notes.
        let ((_, inner), outer) = capture(|| {
            let _ = g.shard_of(&KeyValue::Int(1));
            capture(|| g.shard_of(&KeyValue::Int(2)))
        });
        assert_eq!((inner.len(), outer.len()), (1, 1));
        assert!(capture(|| ()).1.is_empty());
    }
}
