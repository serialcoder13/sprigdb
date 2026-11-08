//! Shard ownership history: the handoff records moves write (chapter 38,
//! "Catalog tables", `feed_ownership`; chapter 10, "The feed handoff").
//!
//! Every committed move of a tenant's range writes one [`Handoff`]: every
//! change on the source up to `fence_pos` (`G`) belongs to the old owner,
//! every change on the target after `start_pos` (`H`) to the new one;
//! between them the target's binlog holds only the mover's own writes.
//!
//! **As built, against the book:** the record lives in the DC tier of
//! etcd, at `feed/ownership/<tenant>/<group>/<map version>`, and the mover
//! writes it in the same etcd transaction as the map flip, not as a
//! post-cutover step into a catalog table. A feed can therefore never see
//! the new owner without the handoff, and can read it while the move's
//! fence still renames the catalog.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_binlog::GtidPos;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_meta::{Key, ListOpts, MetaStore, Op, Record, Tier, Versioned};

/// One committed move of a range (R1: a whole tenant, group 0).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Handoff {
    /// The tenant.
    pub tenant: TenantId,
    /// The table group (0: the catalog map, which R0 and R1 route the
    /// whole tenant by).
    pub group: u32,
    /// The shard map version after the flip.
    pub map_version: u64,
    /// The old owner.
    pub from_cluster: ClusterId,
    /// `G`: the last source position the old owner wrote the range at.
    pub fence_pos: String,
    /// The new owner.
    pub to_cluster: ClusterId,
    /// `H`: the target position after the mover's last write.
    pub start_pos: String,
    /// Every clamp the source gave up to `G` is below this.
    pub hlc_floor: u64,
    /// The mover job.
    pub job_id: String,
    /// When the flip committed (ms since the Unix epoch).
    pub committed_at_ms: u64,
}

impl Versioned for Handoff {
    const V: u32 = 1;
}

impl Handoff {
    /// `G`, parsed.
    ///
    /// # Errors
    /// A malformed position (a bug in the writer).
    pub fn fence(&self) -> Result<GtidPos> {
        self.fence_pos.parse()
    }

    /// `H`, parsed.
    ///
    /// # Errors
    /// A malformed position.
    pub fn start(&self) -> Result<GtidPos> {
        self.start_pos.parse()
    }
}

/// The record's key.
///
/// # Errors
/// `Internal` for an invalid key (a bug).
pub fn key(tier: &Tier, tenant: TenantId, group: u32, map_version: u64) -> Result<Key> {
    tier.key(&format!(
        "feed/ownership/{tenant}/{group}/{map_version:020}"
    ))
}

/// The prefix of a range's records, in map-version order.
///
/// # Errors
/// `Internal` for an invalid key.
pub fn prefix(tier: &Tier, tenant: TenantId, group: u32) -> Result<Key> {
    tier.key(&format!("feed/ownership/{tenant}/{group}/"))
}

/// The put that records `h`, for the flip's etcd transaction.
///
/// # Errors
/// Encoding errors.
pub fn put(tier: &Tier, h: &Handoff) -> Result<Op> {
    Ok(Op::Put {
        key: key(tier, h.tenant, h.group, h.map_version)?,
        value: Record::new(h.clone()).encode()?,
        lease: None,
    })
}

/// Every handoff of a range, oldest first.
///
/// # Errors
/// The store's errors; `Internal` for an unreadable record.
pub async fn history(
    store: &Arc<dyn MetaStore>,
    tenant: TenantId,
    group: u32,
) -> Result<Vec<Handoff>> {
    let p = prefix(store.tier(), tenant, group)?;
    let mut out = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = store.list(&p, opts.clone()).await?;
        for kv in &page.kvs {
            out.push(Record::<Handoff>::decode(&kv.value)?.body);
        }
        if !page.more {
            break;
        }
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
        if opts.after.is_none() {
            break;
        }
    }
    out.sort_by_key(|h| h.map_version);
    if out.windows(2).any(|w| w[0].to_cluster != w[1].from_cluster) {
        return Err(SprigError::internal(format!(
            "tenant {tenant}'s handoff records don't chain"
        )));
    }
    Ok(out)
}
