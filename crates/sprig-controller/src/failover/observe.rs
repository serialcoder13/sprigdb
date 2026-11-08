//! The observer's side of the global decision (chapter 25, "The global
//! failover decision", step 4).
//!
//! ```text
//! ObserveDc(X): probe X failover.observer_probes times, failover.observer_probe_interval_ms
//! apart: X's controllers (Peer.Ping), and a TCP connect plus MySQL handshake to each
//! writer of X's clusters (no credentials needed). Verdict:
//!   alive  : a controller of X reports valid permits
//!   down   : otherwise, if every reachable writer self-fences (agent-managed) or X's
//!            controller reported it fenced
//!   unsafe : otherwise, listing those writers
//! ```
//!
//! **Where it differs from the book.** The book also asks X's gateways.
//! Gateways hold permits only while a controller of X renews them, so
//! asking the controllers is enough; the probe set is in
//! `/sprig/g/dc-observe/<dc>` ([`ObserveTargets`]), which each DC's leader
//! keeps current (X's own tier is out of reach when X is lost).

use std::collections::BTreeSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_common::ids::DcId;
use sprig_meta::{Key, Tier, Versioned};

use super::record::{StandbyLag, Verdict, VerdictKind};

/// `dc-observe/<dc>` in the global tier.
///
/// # Errors
/// Internal never in practice.
pub fn targets_key(dc: &DcId) -> Result<Key> {
    Tier::Global.key(&format!("dc-observe/{dc}"))
}

/// One writer an observer probes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterTarget {
    /// The cluster.
    pub cluster: String,
    /// `host:port`, as other DCs reach it.
    pub addr: String,
    /// The writer's agent fences it when its permit lapses (managed
    /// clusters); external writers can't.
    pub self_fences: bool,
}

impl WriterTarget {
    /// `cluster/addr`, the form verdicts list.
    #[must_use]
    pub fn label(&self) -> String {
        format!("{}/{}", self.cluster, self.addr)
    }
}

/// What observers probe for one DC: `/sprig/g/dc-observe/<dc>`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ObserveTargets {
    /// Base URLs of the DC's controllers' internal service.
    #[serde(default)]
    pub controllers: Vec<String>,
    /// The writers of its clusters.
    #[serde(default)]
    pub writers: Vec<WriterTarget>,
}

impl Versioned for ObserveTargets {
    const V: u32 = 1;
}

/// `Peer.Ping`'s reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerPing {
    /// The answering controller's DC.
    pub dc_id: DcId,
    /// `dcs/<dc>.state` as it last read it.
    pub dc_state: String,
    /// The DC epoch of its lease.
    pub dc_epoch: u64,
    /// It would grant a valid permit now.
    pub permits_valid: bool,
    /// Writers it fenced itself (external clusters that allow it).
    #[serde(default)]
    pub fenced_writers: Vec<String>,
}

/// One probe round of a DC.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProbeRound {
    /// Each answering controller's reply.
    pub pings: Vec<PeerPing>,
    /// Each writer, and whether it answered with a MySQL handshake.
    pub writers: Vec<(WriterTarget, bool)>,
}

/// Probes a DC once.
#[async_trait::async_trait]
pub trait DcProbe: Send + Sync {
    /// One round of probes of `dc`.
    async fn probe(&self, dc: &DcId) -> ProbeRound;
}

/// Runs the observation (see the module docs). `lease_valid` is whether
/// this observer's own DC lease is valid; `lags` the standby lags this
/// site holds.
pub async fn observe(
    me: &DcId,
    lease_valid: bool,
    x: &DcId,
    probe: &dyn DcProbe,
    probes: u32,
    interval: Duration,
    lags: Vec<StandbyLag>,
) -> Verdict {
    let mut alive = false;
    let mut reported: BTreeSet<String> = BTreeSet::new();
    let mut reachable: Vec<WriterTarget> = Vec::new();
    for i in 0..probes.max(1) {
        if i > 0 {
            tokio::time::sleep(interval).await;
        }
        let r = probe.probe(x).await;
        if r.pings.iter().any(|p| p.permits_valid) {
            alive = true;
            break;
        }
        for p in &r.pings {
            reported.extend(p.fenced_writers.iter().cloned());
        }
        for (w, up) in r.writers {
            if up && !reachable.contains(&w) {
                reachable.push(w);
            }
        }
    }
    let unsafe_writers: Vec<String> = reachable
        .iter()
        .filter(|w| !w.self_fences && !reported.contains(&w.label()))
        .map(WriterTarget::label)
        .collect();
    let verdict = if alive {
        VerdictKind::Alive
    } else if unsafe_writers.is_empty() {
        VerdictKind::Down
    } else {
        VerdictKind::Unsafe
    };
    Verdict {
        observer_dc: me.clone(),
        observer_lease_valid: lease_valid,
        verdict,
        unsafe_writers: if alive { Vec::new() } else { unsafe_writers },
        lags,
    }
}

/// Whether `addr` answers a TCP connect with a MySQL handshake (protocol
/// version 10) within `timeout`. No credentials are sent.
pub async fn mysql_handshake(addr: &str, timeout: Duration) -> bool {
    use tokio::io::AsyncReadExt;
    let go = async {
        let mut s = tokio::net::TcpStream::connect(addr).await.ok()?;
        let mut head = [0u8; 5];
        s.read_exact(&mut head).await.ok()?;
        // 3-byte length, sequence id, then the protocol version byte.
        Some(head[4] == 10)
    };
    matches!(tokio::time::timeout(timeout, go).await, Ok(Some(true)))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct Scripted(Mutex<Vec<ProbeRound>>);

    #[async_trait::async_trait]
    impl DcProbe for Scripted {
        async fn probe(&self, _dc: &DcId) -> ProbeRound {
            let mut v = self.0.lock().expect("lock");
            if v.len() > 1 { v.remove(0) } else { v[0].clone() }
        }
    }

    fn dc(s: &str) -> DcId {
        DcId::new(s).expect("dc")
    }

    fn ping(valid: bool) -> PeerPing {
        PeerPing {
            dc_id: dc("fra1"),
            dc_state: "active".into(),
            dc_epoch: 4,
            permits_valid: valid,
            fenced_writers: vec![],
        }
    }

    fn w(addr: &str, self_fences: bool) -> WriterTarget {
        WriterTarget {
            cluster: "fra1-c1".into(),
            addr: addr.into(),
            self_fences,
        }
    }

    async fn run(rounds: Vec<ProbeRound>) -> Verdict {
        let p = Scripted(Mutex::new(rounds));
        observe(&dc("ams1"), true, &dc("fra1"), &p, 3, Duration::ZERO, vec![]).await
    }

    #[tokio::test]
    async fn a_valid_permit_anywhere_means_alive() {
        let v = run(vec![
            ProbeRound::default(),
            ProbeRound { pings: vec![ping(true)], writers: vec![] },
        ])
        .await;
        assert_eq!(v.verdict, VerdictKind::Alive);
    }

    #[tokio::test]
    async fn fenced_or_silent_with_self_fencing_writers_is_down() {
        let v = run(vec![ProbeRound {
            pings: vec![ping(false)],
            writers: vec![(w("10.0.0.1:3306", true), true)],
        }])
        .await;
        assert_eq!(v.verdict, VerdictKind::Down);
        let v = run(vec![ProbeRound::default()]).await;
        assert_eq!(v.verdict, VerdictKind::Down);
    }

    #[tokio::test]
    async fn a_reachable_writer_that_cannot_self_fence_is_unsafe() {
        let v = run(vec![ProbeRound {
            pings: vec![],
            writers: vec![(w("10.0.0.9:3306", false), true)],
        }])
        .await;
        assert_eq!(v.verdict, VerdictKind::Unsafe);
        assert_eq!(v.unsafe_writers, vec!["fra1-c1/10.0.0.9:3306".to_owned()]);
        // Unless X's controller reported it fenced.
        let mut p = ping(false);
        p.fenced_writers = vec!["fra1-c1/10.0.0.9:3306".into()];
        let v = run(vec![ProbeRound {
            pings: vec![p],
            writers: vec![(w("10.0.0.9:3306", false), true)],
        }])
        .await;
        assert_eq!(v.verdict, VerdictKind::Down);
    }

    #[tokio::test]
    async fn a_closed_port_is_no_handshake() {
        assert!(!mysql_handshake("127.0.0.1:1", Duration::from_millis(200)).await);
    }
}
