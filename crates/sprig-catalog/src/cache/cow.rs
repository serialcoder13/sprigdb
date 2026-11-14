//! A hash map shared between catalog snapshots, copied per shard on write.
//!
//! An incremental load derives a new snapshot from the previous one. With
//! plain maps that copies every row; [`CowMap`] splits a large map into
//! [`SHARDS`] shards behind `Arc`s, so a change copies only the shards it
//! touches (the scale target: one incremental change on a
//! 100,000-object catalog in under 20 ms). Small maps, the common case
//! across 20,000 cached tenants, use one shard and cost one allocation.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, RandomState};
use std::sync::Arc;

/// Shards of a large map.
pub(crate) const SHARDS: usize = 64;
/// Size from which a map is split into shards.
const SPLIT_AT: usize = 2048;
/// Size below which a split map is merged back into one shard.
const MERGE_AT: usize = 512;

/// A hash map whose clones share shards until written.
#[derive(Clone)]
pub(crate) struct CowMap<K, V> {
    shards: Vec<Arc<HashMap<K, V>>>,
    len: usize,
    hasher: RandomState,
}

impl<K, V> std::fmt::Debug for CowMap<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CowMap")
            .field("len", &self.len)
            .field("shards", &self.shards.len())
            .finish_non_exhaustive()
    }
}

impl<K: Hash + Eq + Clone, V: Clone> Default for CowMap<K, V> {
    fn default() -> Self {
        Self {
            shards: vec![Arc::default()],
            len: 0,
            hasher: RandomState::new(),
        }
    }
}

impl<K: Hash + Eq + Clone, V: Clone> CowMap<K, V> {
    fn shard_of<Q: Hash + ?Sized>(&self, k: &Q) -> usize {
        if self.shards.len() == 1 {
            return 0;
        }
        // The modulus keeps the value below SHARDS, so it fits.
        #[allow(clippy::cast_possible_truncation)]
        let i = (self.hasher.hash_one(k) % self.shards.len() as u64) as usize;
        i
    }

    /// The value of `k`.
    pub(crate) fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.shards[self.shard_of(k)].get(k)
    }

    /// Inserts, copying the shard if another snapshot shares it.
    pub(crate) fn insert(&mut self, k: K, v: V) -> Option<V> {
        let i = self.shard_of(&k);
        let old = Arc::make_mut(&mut self.shards[i]).insert(k, v);
        if old.is_none() {
            self.len += 1;
        }
        old
    }

    /// Removes, copying the shard only if the key is there.
    pub(crate) fn remove<Q>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let i = self.shard_of(k);
        if !self.shards[i].contains_key(k) {
            return None;
        }
        let old = Arc::make_mut(&mut self.shards[i]).remove(k);
        if old.is_some() {
            self.len -= 1;
        }
        old
    }

    /// Number of entries.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Whether the map is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Every entry, in no particular order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.shards.iter().flat_map(|s| s.iter())
    }

    /// Every value, in no particular order.
    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.shards.iter().flat_map(|s| s.values())
    }

    /// Splits or merges shards after a batch of writes, by size.
    pub(crate) fn rebalance(&mut self) {
        let want = if self.len >= SPLIT_AT {
            SHARDS
        } else if self.len < MERGE_AT {
            1
        } else {
            self.shards.len()
        };
        if want == self.shards.len() {
            return;
        }
        let entries: Vec<(K, V)> = self.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let mut shards: Vec<HashMap<K, V>> = (0..want).map(|_| HashMap::new()).collect();
        self.shards = vec![Arc::default(); want];
        for (k, v) in entries {
            let i = self.shard_of(&k);
            shards[i].insert(k, v);
        }
        self.shards = shards.into_iter().map(Arc::new).collect();
    }

    /// Number of shards not shared with `other` (tests).
    #[cfg(test)]
    pub(crate) fn unshared(&self, other: &Self) -> usize {
        self.shards
            .iter()
            .zip(&other.shards)
            .filter(|(a, b)| !Arc::ptr_eq(a, b))
            .count()
            + self.shards.len().abs_diff(other.shards.len())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn writes_copy_only_their_shard_and_maps_rebalance() {
        let mut m: CowMap<u64, u64> = CowMap::default();
        for i in 0..5000 {
            m.insert(i, i * 2);
        }
        m.rebalance();
        assert_eq!(m.shards.len(), SHARDS);
        assert_eq!(m.len(), 5000);
        assert_eq!(m.get(&4999), Some(&9998));

        let mut n = m.clone();
        n.insert(7, 0);
        assert_eq!(n.unshared(&m), 1);
        assert_eq!(m.get(&7), Some(&14));
        assert_eq!(n.get(&7), Some(&0));
        assert_eq!(n.remove(&123_456), None);
        assert_eq!(n.unshared(&m), 1);

        for i in 0..4800 {
            n.remove(&i);
        }
        n.rebalance();
        assert_eq!(n.shards.len(), 1);
        assert_eq!(n.len(), 200);
        assert_eq!(n.iter().count(), 200);
        assert!(n.values().all(|v| *v >= 9600));
        assert!(!n.is_empty());
    }
}
