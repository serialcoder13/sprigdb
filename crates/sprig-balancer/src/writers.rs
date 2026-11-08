//! Galera writer weights (chapter 11, "Scheduling, guardrails and
//! recommendations"; chapter 09's writer choice): when one node's flow
//! control or `Threads_running` exceeds 1.5 times its peers' average for
//! 10 minutes, its weight drops by 0.25 (floor 0.25), recovering by 0.25
//! per hour.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::Timestamp;

/// Above this multiple of the peers' average, a node is overloaded.
pub const OVERLOAD_FACTOR: f64 = 1.5;
/// It must stay overloaded this long.
pub const OVERLOAD_SECS: u64 = 600;
/// The step down and up.
pub const STEP: f64 = 0.25;
/// The lowest weight.
pub const FLOOR: f64 = 0.25;
/// Time between recovery steps.
pub const RECOVER_SECS: u64 = 3600;

/// One node's load sample.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct NodeSample {
    /// Flow control paused ratio.
    pub flow_control: f64,
    /// `Threads_running`.
    pub threads_running: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct NodeState {
    weight: f64,
    overloaded_since: Option<Timestamp>,
    last_change: Timestamp,
}

/// Writer weights of one Galera cluster.
#[derive(Debug, Clone, Default)]
pub struct WriterWeights {
    nodes: BTreeMap<String, NodeState>,
}

impl WriterWeights {
    /// No history: every node at weight 1.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes one sample per node at `now`; returns the weights (1.0 for a
    /// node never seen overloaded).
    pub fn observe(&mut self, samples: &BTreeMap<String, NodeSample>, now: Timestamp) -> BTreeMap<String, f64> {
        let n = samples.len();
        for (node, s) in samples {
            let st = self.nodes.entry(node.clone()).or_insert(NodeState {
                weight: 1.0,
                overloaded_since: None,
                last_change: now,
            });
            #[allow(clippy::cast_precision_loss)]
            let overloaded = n > 1 && {
                let peers = (n - 1) as f64;
                let fc: f64 = samples
                    .iter()
                    .filter(|(k, _)| *k != node)
                    .map(|(_, x)| x.flow_control)
                    .sum::<f64>()
                    / peers;
                let th: f64 = samples
                    .iter()
                    .filter(|(k, _)| *k != node)
                    .map(|(_, x)| x.threads_running)
                    .sum::<f64>()
                    / peers;
                (s.flow_control > OVERLOAD_FACTOR * fc && s.flow_control > 0.0)
                    || (s.threads_running > OVERLOAD_FACTOR * th && s.threads_running > 0.0)
            };
            if overloaded {
                let since = *st.overloaded_since.get_or_insert(now);
                if now >= since + OVERLOAD_SECS && now >= st.last_change + OVERLOAD_SECS {
                    st.weight = (st.weight - STEP).max(FLOOR);
                    st.last_change = now;
                    st.overloaded_since = Some(now);
                }
            } else {
                st.overloaded_since = None;
                if st.weight < 1.0 && now >= st.last_change + RECOVER_SECS {
                    st.weight = (st.weight + STEP).min(1.0);
                    st.last_change = now;
                }
            }
        }
        self.nodes.retain(|k, _| samples.contains_key(k));
        self.nodes.iter().map(|(k, s)| (k.clone(), s.weight)).collect()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::float_cmp)]

    use super::*;

    fn samples(hot: f64) -> BTreeMap<String, NodeSample> {
        [("n1", hot), ("n2", 4.0), ("n3", 4.0)]
            .into_iter()
            .map(|(n, t)| {
                (
                    n.to_owned(),
                    NodeSample {
                        flow_control: 0.0,
                        threads_running: t,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn overloaded_node_steps_down_to_the_floor_and_recovers() {
        let mut w = WriterWeights::new();
        let mut now = 0;
        assert_eq!(w.observe(&samples(10.0), now)["n1"], 1.0);
        now += 600;
        assert_eq!(w.observe(&samples(10.0), now)["n1"], 0.75);
        for _ in 0..10 {
            now += 600;
            w.observe(&samples(10.0), now);
        }
        assert_eq!(w.observe(&samples(10.0), now)["n1"], FLOOR);
        assert_eq!(w.observe(&samples(10.0), now)["n2"], 1.0);
        // Balanced again: +0.25 per hour.
        now += 3600;
        assert_eq!(w.observe(&samples(4.0), now)["n1"], 0.5);
        now += 1800;
        assert_eq!(w.observe(&samples(4.0), now)["n1"], 0.5);
        now += 1800;
        assert_eq!(w.observe(&samples(4.0), now)["n1"], 0.75);
    }

    #[test]
    fn a_short_spike_changes_nothing() {
        let mut w = WriterWeights::new();
        w.observe(&samples(10.0), 0);
        w.observe(&samples(10.0), 300);
        assert_eq!(w.observe(&samples(4.0), 900)["n1"], 1.0);
        assert_eq!(w.observe(&samples(10.0), 1000)["n1"], 1.0);
    }
}
