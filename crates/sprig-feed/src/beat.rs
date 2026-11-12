//! The queue forwarder's heartbeat (chapter 50, "Publishing inside a
//! database transaction"; bugfix-42).
//!
//! The worker's `queue_forwarder` job of a cluster writes
//! `queue-forwarder-beats/<cluster>` in its DC's tier every 2 s while it runs
//! (and removes it when it stops). The gateways' polling fallback reads it
//! before taking a transactional publish older than
//! `queue.forward_fallback_ms`: while the cluster's forwarder runs, the
//! feed forwards the row, in commit order, and the fallback leaves it
//! alone for longer (its insert order isn't the commit order, so a row it
//! takes can overtake one committed before it).

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_common::ids::ClusterId;
use sprig_meta::{Consistency, MetaStore, Op, Record, Txn, Versioned};

/// The key's prefix in a DC's tier.
pub const PREFIX: &str = "queue-forwarder-beats/";

/// How often the job writes its beat.
pub const PERIOD_MS: u64 = 2_000;

/// A beat older than this means the cluster's forwarder isn't running
/// (three beats missed).
pub const FRESH_MS: u64 = 3 * PERIOD_MS;

/// `queue-forwarder-beats/<cluster>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Beat {
    /// When written (ms since the Unix epoch).
    pub at_ms: u64,
    /// The tenants the job serves.
    pub tenants: u64,
}

impl Versioned for Beat {
    const V: u32 = 1;
}

impl Beat {
    /// Whether the beat shows a running forwarder at `now_ms`.
    #[must_use]
    pub fn fresh(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.at_ms) <= FRESH_MS
    }
}

/// Writes `cluster`'s beat in `store` (the job's DC tier).
///
/// # Errors
/// The store's.
pub async fn write(store: &Arc<dyn MetaStore>, cluster: &ClusterId, beat: Beat) -> Result<()> {
    let key = store.tier().key(&format!("{PREFIX}{cluster}"))?;
    store
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Put {
                key,
                value: Record::new(beat).encode()?,
                lease: None,
            }],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// Removes `cluster`'s beat (the job stopped).
///
/// # Errors
/// The store's.
pub async fn clear(store: &Arc<dyn MetaStore>, cluster: &ClusterId) -> Result<()> {
    let key = store.tier().key(&format!("{PREFIX}{cluster}"))?;
    store
        .txn(Txn {
            when: Vec::new(),
            then: vec![Op::Delete(key)],
            otherwise: Vec::new(),
        })
        .await?;
    Ok(())
}

/// `cluster`'s beat in `store`, if any.
///
/// # Errors
/// The store's.
pub async fn read(store: &Arc<dyn MetaStore>, cluster: &ClusterId) -> Result<Option<Beat>> {
    let key = store.tier().key(&format!("{PREFIX}{cluster}"))?;
    let Some(kv) = store.get(&key, Consistency::Serializable).await? else {
        return Ok(None);
    };
    Ok(Record::<Beat>::decode(&kv.value).ok().map(|r| r.body))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use sprig_common::ids::DcId;
    use sprig_meta::{MemStore, Tier};

    #[tokio::test]
    async fn a_beat_is_written_read_and_cleared() {
        let store: Arc<dyn MetaStore> =
            Arc::new(MemStore::new(Tier::Dc(DcId::new("dc-a").unwrap())));
        let c = ClusterId::new("dc-a-c1").unwrap();
        assert_eq!(read(&store, &c).await.unwrap(), None);
        let b = Beat {
            at_ms: 10_000,
            tenants: 3,
        };
        write(&store, &c, b).await.unwrap();
        assert_eq!(read(&store, &c).await.unwrap(), Some(b));
        assert!(b.fresh(10_000 + FRESH_MS));
        assert!(!b.fresh(10_001 + FRESH_MS));
        clear(&store, &c).await.unwrap();
        assert_eq!(read(&store, &c).await.unwrap(), None);
    }
}
