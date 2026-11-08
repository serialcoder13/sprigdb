//! The k-way merge across clusters (chapter 38, "Merging across shards").
//!
//! Each input is one cluster's feed of a scope, in key order; its
//! **frontier** is the key of the last thing it yielded, an item or a
//! progress mark (heartbeats keep idle inputs moving). The merge emits the
//! smallest head once every other input's frontier has reached it, so the
//! output is one reproducible order by `(clamp, cluster, row)`. While a
//! range is held by a move, nothing above the hold's floor is emitted, so
//! the order stays causal across the move.
//!
//! R1's tenant feed ([`crate::tail`]) has one input at a time (a tenant
//! lives on one cluster); this is the merge the multi-cluster feed (R5,
//! build step 12) runs.

use std::cmp::Ordering;
use std::collections::VecDeque;

use sprig_common::ids::ClusterId;

/// The order of the merged feed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MergeKey {
    /// The transaction's clamp.
    pub clamp: u64,
    /// Its cluster.
    pub cluster: ClusterId,
    /// The row in the transaction (`u32::MAX`: after it).
    pub row: u32,
}

impl Ord for MergeKey {
    fn cmp(&self, o: &Self) -> Ordering {
        (self.clamp, self.cluster.as_str(), self.row).cmp(&(o.clamp, o.cluster.as_str(), o.row))
    }
}

impl PartialOrd for MergeKey {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

#[derive(Debug)]
struct Input<T> {
    queue: VecDeque<(MergeKey, T)>,
    frontier: Option<MergeKey>,
    closed: bool,
}

/// A merge of `n` inputs.
#[derive(Debug)]
pub struct Merge<T> {
    inputs: Vec<Input<T>>,
    hold_floor: Option<u64>,
}

impl<T> Merge<T> {
    /// A merge of `n` inputs, none heard from yet.
    #[must_use]
    pub fn new(n: usize) -> Self {
        Self {
            inputs: (0..n)
                .map(|_| Input {
                    queue: VecDeque::new(),
                    frontier: None,
                    closed: false,
                })
                .collect(),
            hold_floor: None,
        }
    }

    /// Adds an input; returns its index.
    pub fn add_input(&mut self) -> usize {
        self.inputs.push(Input {
            queue: VecDeque::new(),
            frontier: None,
            closed: false,
        });
        self.inputs.len() - 1
    }

    /// Input `i` yielded `item` at `key` (keys rise per input).
    pub fn push(&mut self, i: usize, key: MergeKey, item: T) {
        if let Some(inp) = self.inputs.get_mut(i) {
            inp.frontier = Some(key.clone());
            inp.queue.push_back((key, item));
        }
    }

    /// Input `i` has nothing below `key` still to come (a heartbeat).
    pub fn progress(&mut self, i: usize, key: MergeKey) {
        if let Some(inp) = self.inputs.get_mut(i)
            && inp.frontier.as_ref().is_none_or(|f| key > *f)
        {
            inp.frontier = Some(key);
        }
    }

    /// Input `i` ended: a source after its fence.
    pub fn close(&mut self, i: usize) {
        if let Some(inp) = self.inputs.get_mut(i) {
            inp.closed = true;
        }
    }

    /// A move holds part of the scope: emit nothing above `floor` until
    /// [`Self::release`].
    pub fn hold(&mut self, floor: u64) {
        self.hold_floor = Some(floor);
    }

    /// The hold ended.
    pub fn release(&mut self) {
        self.hold_floor = None;
    }

    /// The next item in merged order, once it is safe to emit.
    pub fn pop(&mut self) -> Option<(MergeKey, T)> {
        let (best, key) = self
            .inputs
            .iter()
            .enumerate()
            .filter_map(|(i, inp)| inp.queue.front().map(|(k, _)| (i, k)))
            .min_by(|a, b| a.1.cmp(b.1))
            .map(|(i, k)| (i, k.clone()))?;
        if self.hold_floor.is_some_and(|f| key.clamp > f) {
            return None;
        }
        let safe = self.inputs.iter().enumerate().all(|(i, inp)| {
            i == best || inp.closed || inp.frontier.as_ref().is_some_and(|f| *f >= key)
        });
        if !safe {
            return None;
        }
        self.inputs[best].queue.pop_front()
    }

    /// The point every input has passed: a resume token for "everything
    /// emitted so far" (`postBatchResumeToken`). `None` until every open
    /// input has been heard from.
    #[must_use]
    pub fn resume_point(&self) -> Option<MergeKey> {
        let mut out: Option<MergeKey> = None;
        for inp in self.inputs.iter().filter(|i| !i.closed) {
            let f = match inp.queue.front() {
                Some((k, _)) => MergeKey {
                    clamp: k.clamp.saturating_sub(1),
                    cluster: k.cluster.clone(),
                    row: u32::MAX,
                },
                None => inp.frontier.clone()?,
            };
            if out.as_ref().is_none_or(|o| f < *o) {
                out = Some(f);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(clamp: u64, c: &str, row: u32) -> MergeKey {
        MergeKey {
            clamp,
            cluster: ClusterId::new(c).unwrap(),
            row,
        }
    }

    /// The chapter's worked example: c1 has T(105); c2 has heartbeat 103,
    /// T(104), heartbeat 110.
    #[test]
    fn worked_example() {
        let mut m: Merge<&str> = Merge::new(2);
        m.push(0, k(105, "c1", 0), "c1:105");
        m.progress(1, k(103, "c2", u32::MAX));
        assert_eq!(m.pop(), None, "c2 may still yield 104");
        m.push(1, k(104, "c2", 0), "c2:104");
        assert_eq!(m.pop().map(|x| x.1), Some("c2:104"));
        assert_eq!(m.pop(), None, "c2's frontier 104 < 105");
        m.progress(1, k(110, "c2", u32::MAX));
        assert_eq!(m.pop().map(|x| x.1), Some("c1:105"));
        assert_eq!(m.pop(), None);
        assert_eq!(m.resume_point(), Some(k(105, "c1", 0)));
    }

    #[test]
    fn holds_and_closed_inputs() {
        let mut m: Merge<u32> = Merge::new(2);
        m.hold(100);
        m.push(0, k(101, "a", 0), 1);
        m.close(1);
        assert_eq!(m.pop(), None, "held above the floor");
        m.release();
        assert_eq!(m.pop().map(|x| x.1), Some(1));
        let i = m.add_input();
        m.push(0, k(102, "a", 0), 2);
        assert_eq!(m.pop(), None, "the new input hasn't been heard from");
        m.progress(i, k(200, "b", 0));
        assert_eq!(m.pop().map(|x| x.1), Some(2));
    }

    #[test]
    fn output_is_sorted_whatever_the_arrival_order() {
        // Three inputs with interleaved keys, fed in a scrambled order.
        let mut m: Merge<u64> = Merge::new(3);
        let mut out = Vec::new();
        let names = ["a", "b", "c"];
        let mut next = [1u64, 2, 3];
        for step in 0..60u64 {
            let i = usize::try_from((step * 7) % 3).unwrap();
            let c = next[i];
            next[i] += 3;
            m.push(i, k(c, names[i], 0), c);
            while let Some((_, v)) = m.pop() {
                out.push(v);
            }
        }
        for i in 0..3 {
            m.close(i);
        }
        while let Some((_, v)) = m.pop() {
            out.push(v);
        }
        assert_eq!(out.len(), 60);
        assert!(out.windows(2).all(|w| w[0] < w[1]), "{out:?}");
    }
}
