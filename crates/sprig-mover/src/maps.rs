//! [`MapIo`] on chapter 15's R0 tenant map (`shardmaps/<tenant>/0`),
//! through `sprig-shard`'s compare-and-put (chapter 10, "Cutover";
//! chapter 15, "Epochs").

use sprig_common::ids::{ClusterId, JobId, TenantId};
use sprig_common::{ErrorKind, Result, SprigCode};
use sprig_shard::TenantMaps;
use sprig_shard::map::{self, Placement};

use crate::io::{CommitOutcome, MapIo, MapState};

/// One tenant's map, as a job's [`MapIo`].
#[derive(Debug, Clone)]
pub struct TenantMapIo {
    maps: TenantMaps,
    tenant: TenantId,
}

impl TenantMapIo {
    /// `tenant`'s map in `maps`.
    #[must_use]
    pub fn new(maps: TenantMaps, tenant: TenantId) -> Self {
        Self { maps, tenant }
    }
}

fn job_text(job: JobId) -> String {
    job.0.to_string()
}

/// The map already shows `job`'s commit.
fn already_moved(m: &sprig_catalog::placement::ShardMap, job: &str) -> Result<bool> {
    let p = Placement::of(m)?;
    Ok(p.hold.is_none() && p.moved_by.as_deref() == Some(job))
}

#[async_trait::async_trait]
impl MapIo for TenantMapIo {
    async fn hold(&self, job: JobId, until_ms: u64) -> Result<u64> {
        let me = job_text(job);
        let m = self
            .maps
            .update(self.tenant, |m| {
                map::hold(m, &me, until_ms)?;
                Ok(true)
            })
            .await?;
        Ok(Placement::of(&m)?.epoch)
    }

    async fn release(&self, job: JobId) -> Result<()> {
        let me = job_text(job);
        self.maps
            .update(self.tenant, |m| Ok(map::release(m, &me)))
            .await?;
        Ok(())
    }

    async fn commit(&self, job: JobId, to: &ClusterId) -> Result<CommitOutcome> {
        let me = job_text(job);
        match self
            .maps
            .update(self.tenant, |m| {
                if already_moved(m, &me)? {
                    return Ok(false); // a retry after a lost reply
                }
                map::flip(m, &me, to)?;
                Ok(true)
            })
            .await
        {
            Ok(_) => Ok(CommitOutcome::Committed),
            Err(e) if matches!(e.kind, ErrorKind::Sprig(SprigCode::MoveCutoverAborted)) => {
                Ok(CommitOutcome::Rejected)
            }
            Err(e) if e.retryable => {
                tracing::warn!(job = %me, error = %e, "commit outcome unknown");
                Ok(CommitOutcome::Unknown)
            }
            Err(e) => Err(e),
        }
    }

    async fn bump(&self, job: JobId) -> Result<CommitOutcome> {
        let me = job_text(job);
        let r = self
            .maps
            .update(self.tenant, |m| {
                if already_moved(m, &me)? {
                    return Ok(false);
                }
                let cluster = Placement::of(m)?.cluster;
                map::flip(m, &me, &cluster)?;
                Ok(true)
            })
            .await;
        match r {
            Ok(_) => Ok(CommitOutcome::Committed),
            Err(e) if matches!(e.kind, ErrorKind::Sprig(SprigCode::MoveCutoverAborted)) => {
                Ok(CommitOutcome::Rejected)
            }
            Err(e) => Err(e),
        }
    }

    async fn read(&self) -> Result<MapState> {
        let (m, _) = self.maps.load(self.tenant).await?;
        let p = Placement::of(&m)?;
        Ok(MapState {
            cluster: p.cluster,
            epoch: p.epoch,
            held_by: p.hold.map(|h| h.job),
            moved_by: p.moved_by,
        })
    }
}
