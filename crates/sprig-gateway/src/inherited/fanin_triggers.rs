//! Fan-in triggers (chapter 41, "Fan-in triggers"; chapter 42, "Fan-in";
//! R9, r9next): `CREATE TRIGGER g ON STREAM s FAN IN` in tenant P runs once,
//! in P, with P's definer, over every member instance of the inherited
//! stream `s` below P.
//!
//! ```text
//! declare (P, binding b, definition D):
//!   SPRIG_FAN_IN and SELECT on s in P; the source row's object id carries D
//!   inherit-fanin-triggers/<D>/<P>.<b>     (sprig_logic::messages)
//!   each instance of D below P (residency allowing): its positions at its
//!   high watermarks (records published from now on), then
//!   logic-fanin/<P>/<b>/<member>
//! a new instance of D in member T (after its first use, streams::after_use):
//!   for each fan-in trigger of D whose tenant is an ancestor of T:
//!   logic-fanin/<P>/<b>/<T>, no position: the worker reads it from its
//!   start (as a member's inherited trigger on a new instance does)
//! run: the worker's logic_stream_trigger job reads every listed instance
//!   (member's log, P's positions with instance_tenant_id = T) and sends
//!   each record to a gateway as P's definer; the run's offset commit names
//!   the member (sprig_streams::offsets::PROCEDURE_FAN_IN)
//! ```

use std::sync::Arc;

use sprig_common::Result;
use sprig_common::ids::TenantId;
use sprig_inherit::msg::{Chain, DefKind, Definition};
use sprig_logic::messages::{self, FanInMember, FanInTrigger};
use sprig_streams::offsets;

use super::catalog;
use super::streams::StreamInstance;
use crate::tenant::TenantServices;

/// Whether `t`'s records may be read by a fan-in reader in another tenant
/// (no `residency.deny_remote_access`).
async fn readable(svc: &TenantServices, t: TenantId) -> Result<bool> {
    let Some(d) = super::dir_entry(svc, t).await? else {
        return Ok(false);
    };
    let deny = sprig_auth::TenantResidency::of(&d.residency).is_some_and(|r| r.deny_remote_access);
    if deny {
        metrics::counter!("sprig_inherit_fanin_skipped_total", "reason" => "residency")
            .increment(1);
    }
    Ok(!deny)
}

/// Lists fan-in trigger `binding` of `p` under `def`, and every existing
/// instance below `p` with its positions at the high watermarks. The
/// number of instances attached.
///
/// # Errors
/// The store's, the catalogs' and the clusters'.
#[allow(clippy::many_single_char_names)]
pub async fn attach_existing(
    svc: &Arc<TenantServices>,
    p: TenantId,
    binding: u64,
    def: &Definition,
) -> Result<usize> {
    let store = svc.locator.dc_store();
    messages::put_fanin_trigger(
        store.as_ref(),
        (def.declarer.0, def.object_id),
        &FanInTrigger { tenant: p, binding },
    )
    .await?;
    let schema = {
        use sprig_catalog::CatalogLocator;
        svc.locator.writer(p).await?.store.schema().to_owned()
    };
    let mut n = 0;
    for e in super::instances_of(svc, def.key()).await? {
        if e.kind != DefKind::Stream || e.tenant == p {
            continue;
        }
        let Some(d) = super::dir_entry(svc, e.tenant).await? else {
            continue;
        };
        if !d.path_ids().contains(&p.0) || !readable(svc, e.tenant).await? {
            continue;
        }
        // Its positions at the high watermarks: records from now on.
        let tcat = catalog(svc, e.tenant).await?;
        let Some(s) = tcat
            .ext
            .get::<sprig_streams::StreamsState>(sprig_streams::MODULE)
            .and_then(|st| st.all().find(|s| s.id.0 == e.instance_id).cloned())
        else {
            continue;
        };
        let mut start = Vec::new();
        if let Some(log) = s.log.first() {
            let cluster = svc.locator.cluster(e.tenant).await?;
            let pool = svc.locator.ctl_pool(&cluster).await?;
            let mut c = pool
                .get_conn()
                .await
                .map_err(sprig_streams::maria::map_err)?;
            let reader = sprig_streams::read::LogReader {
                schema: log.slot.schema(e.tenant, s.db).to_string(),
            };
            for part in 0..s.partitions {
                let hw = reader
                    .bounds(&mut c, s.id, sprig_streams::PartitionNo(part))
                    .await
                    .map_or(0, |b| b.high_watermark.0);
                start.push((part, hw));
            }
        }
        {
            use sprig_catalog::CatalogLocator;
            let ep = svc.locator.writer(p).await?;
            let mut c = ep
                .pool
                .get_conn()
                .await
                .map_err(sprig_streams::maria::map_err)?;
            sprig_streams::store::migrate(&mut c, &schema).await?;
            offsets::init_positions_of(
                &mut c,
                &schema,
                &offsets::logic_group(binding),
                e.tenant.0,
                s.id.0,
                &start,
            )
            .await?;
        }
        messages::put_fanin_member(
            store.as_ref(),
            &FanInMember {
                tenant: p,
                binding,
                member: e.tenant,
                stream: s.id.0,
            },
        )
        .await?;
        n += 1;
    }
    Ok(n)
}

/// After a use of member `si.tenant`'s instance of `def`: the fan-in
/// triggers of `def` in its ancestors read it (see the module docs).
/// Failures are logged; the next use tries again.
pub async fn on_instance(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    def: &Definition,
    si: &StreamInstance,
) {
    let r = async {
        let store = svc.locator.dc_store();
        let list =
            messages::fanin_triggers(store.as_ref(), (def.declarer.0, def.object_id)).await?;
        if list.is_empty() {
            return Ok::<_, sprig_common::SprigError>(());
        }
        let anc = chain.ancestry();
        let mut checked = None;
        for t in list {
            if !anc.contains(&t.tenant) {
                continue;
            }
            if checked.is_none() {
                checked = Some(readable(svc, si.tenant).await?);
            }
            if checked != Some(true) {
                return Ok(());
            }
            messages::put_fanin_member(
                store.as_ref(),
                &FanInMember {
                    tenant: t.tenant,
                    binding: t.binding,
                    member: si.tenant,
                    stream: si.stream.id.0,
                },
            )
            .await?;
        }
        Ok(())
    }
    .await;
    if let Err(e) = r {
        tracing::warn!(tenant = %si.tenant, def = %def.key(), error = %e, "fan-in triggers not told of an instance; the next use retries");
    }
}
