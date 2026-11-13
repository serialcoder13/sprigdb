//! Backup records, exported for the `BackupMissed` alert.
//!
//! The DC's backups don't run in a SprigDB process: the Compose bundle's
//! `sprig-vm.sh backup` takes them (from cron or a timer). When one
//! completes, the tool records it in the DC tier:
//!
//! ```text
//! backups/<cluster>/<kind>   {"v":1,"finished_at":<unix seconds>,"name":"<backup name>"}
//! ```
//!
//! Every controller reads those records once a minute and exports
//! `sprig_backup_last_success_timestamp_seconds{cluster, kind}`, so the
//! alert sees the newest successful backup of each cluster whichever
//! controller Prometheus scrapes. A record that doesn't decode is logged
//! and skipped.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use sprig_common::{CancellationToken, Result};
use sprig_meta::{KeyValue, ListOpts, MetaStore};

/// The records' prefix in the DC tier.
pub const PREFIX: &str = "backups/";

/// How often the records are read.
pub const PERIOD: Duration = Duration::from_secs(60);

/// One backup record.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BackupRecord {
    /// When the backup finished, in Unix seconds.
    pub finished_at: f64,
}

/// One cluster's newest backup of one kind.
#[derive(Debug, Clone, PartialEq)]
pub struct LastBackup {
    /// The cluster, as the record's key names it.
    pub cluster: String,
    /// `full`, or another kind the tool records.
    pub kind: String,
    /// When it finished, in Unix seconds.
    pub finished_at: f64,
}

/// Every well-formed record under [`PREFIX`].
///
/// # Errors
/// The store's errors.
pub async fn read(store: &dyn MetaStore) -> Result<Vec<LastBackup>> {
    let prefix = store.tier().key(PREFIX)?;
    let mut kvs: Vec<KeyValue> = Vec::new();
    let mut opts = ListOpts::default();
    loop {
        let page = store.list(&prefix, opts.clone()).await?;
        let more = page.more;
        opts.at = Some(page.revision);
        opts.after = page.kvs.last().map(|kv| kv.key.clone());
        kvs.extend(page.kvs);
        if !more {
            break;
        }
    }
    let mut out = Vec::new();
    for kv in kvs {
        let rel = &kv.key.as_str()[prefix.as_str().len()..];
        let Some((cluster, kind)) = rel.split_once('/') else {
            tracing::warn!(
                key = kv.key.as_str(),
                "backup record key isn't backups/<cluster>/<kind>; skipped"
            );
            continue;
        };
        if cluster.is_empty() || kind.is_empty() || kind.contains('/') {
            tracing::warn!(
                key = kv.key.as_str(),
                "backup record key isn't backups/<cluster>/<kind>; skipped"
            );
            continue;
        }
        match serde_json::from_slice::<BackupRecord>(&kv.value) {
            Ok(r) if r.finished_at.is_finite() && r.finished_at > 0.0 => out.push(LastBackup {
                cluster: cluster.to_owned(),
                kind: kind.to_owned(),
                finished_at: r.finished_at,
            }),
            Ok(_) => {
                tracing::warn!(
                    key = kv.key.as_str(),
                    "backup record has no valid finished_at; skipped"
                );
            }
            Err(e) => {
                tracing::warn!(key = kv.key.as_str(), error = %e, "backup record doesn't decode; skipped");
            }
        }
    }
    Ok(out)
}

/// Reads the records and sets the gauge for each.
///
/// # Errors
/// The store's errors.
pub async fn export_once(store: &dyn MetaStore) -> Result<usize> {
    let all = read(store).await?;
    for b in &all {
        metrics::gauge!("sprig_backup_last_success_timestamp_seconds", "cluster" => b.cluster.clone(), "kind" => b.kind.clone())
            .set(b.finished_at);
    }
    Ok(all.len())
}

/// Exports the records every [`PERIOD`] until `shutdown`.
pub fn spawn(
    store: Arc<dyn MetaStore>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PERIOD);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = tick.tick() => {}
            }
            if let Err(e) = export_once(&*store).await {
                tracing::warn!(error = %e, "backup records not read; the backup gauge keeps its last values");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_common::ids::DcId;
    use sprig_meta::{MemStore, Op, Tier, Txn};

    async fn put(store: &MemStore, rel: &str, value: &str) {
        let key = store.tier().key(rel).unwrap();
        store
            .txn(Txn {
                then: vec![Op::Put {
                    key,
                    value: value.as_bytes().to_vec(),
                    lease: None,
                }],
                ..Txn::default()
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reads_each_cluster_and_kind_and_skips_bad_records() {
        let store = MemStore::new(Tier::Dc(DcId::new("fra1").unwrap()));
        put(
            &store,
            "backups/fra1-c1/full",
            r#"{"v":1,"finished_at":1763000000,"name":"20261113T000000Z"}"#,
        )
        .await;
        put(
            &store,
            "backups/fra1-c2/full",
            r#"{"v":1,"finished_at":1763003600.5}"#,
        )
        .await;
        put(&store, "backups/fra1-c3/full", r#"{"v":1}"#).await;
        put(&store, "backups/fra1-c4/full", "not json").await;
        put(&store, "backups/fra1-c5", r#"{"finished_at":1}"#).await;
        put(&store, "clusters/fra1-c1", r#"{"finished_at":1}"#).await;
        let got = read(&store).await.unwrap();
        assert_eq!(
            got,
            vec![
                LastBackup {
                    cluster: "fra1-c1".into(),
                    kind: "full".into(),
                    finished_at: 1_763_000_000.0
                },
                LastBackup {
                    cluster: "fra1-c2".into(),
                    kind: "full".into(),
                    finished_at: 1_763_003_600.5
                },
            ]
        );
        assert_eq!(export_once(&store).await.unwrap(), 2);
    }
}
